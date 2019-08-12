package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	flow "github.com/bwNetFlow/protobuf/go"
)

// Connector handles a connection to read bwNetFlow flows from kafka.
type Connector struct {
	user                       string
	pass                       string
	consumer                   *cluster.Consumer
	producer                   sarama.AsyncProducer
	consumerChannel            chan *flow.FlowMessage
	hasConsumerControlListener bool
	consumerControlChannel     chan ConsumerControlMessage
	producerChannels           map[string](chan *flow.FlowMessage)
	manualErrFlag              bool
	manualErrSignal            chan bool
	channelLength              uint
}

// ConsumerControlMessage takes the control params of *sarama.ConsumerMessage
type ConsumerControlMessage struct {
	Partition      int32
	Offset         int64
	Timestamp      time.Time // only set if kafka is version 0.10+, inner message timestamp
	BlockTimestamp time.Time // only set if kafka is version 0.10+, outer (compressed) block timestamp
}

// SetAuth explicitly set which login to use in SASL/PLAIN auth via TLS
func (connector *Connector) SetAuth(user string, pass string) {
	connector.user = user
	connector.pass = pass
}

// Check environment to infer which login to use in SASL/PLAIN auth via TLS
// Requires KAFKA_SASL_USER and KAFKA_SASL_PASS to be set for this process.
func (connector *Connector) SetAuthFromEnv() error {
	connector.user = os.Getenv("KAFKA_SASL_USER")
	connector.pass = os.Getenv("KAFKA_SASL_PASS")
	if connector.user == "" || connector.pass == "" {
		return errors.New("Setting Kafka SASL info from Environment was unsuccessful.")
	}
	return nil
}

// Set anonymous credentials as login method.
func (connector *Connector) SetAuthAnon() {
	connector.user = "anon"
	connector.pass = "anon"
}

// Enable manual error handling by setting the internal flags.
// Any application calling this will have to read all messages provided by the
// channels returned from the ConsumerErrors, ConsumerNotifications and
// ProducerErrors methods. Else there will be deadlocks.
//
// If this is called before any `.Start*` method was called, no go routines
// will be spawned for logging any messages. This is the recommended case.
// If this is called after any `.Start*` method was called, spawned go routines
// will be terminated.
func (connector *Connector) EnableManualErrorHandling() {
	connector.manualErrFlag = true
	if connector.manualErrSignal != nil {
		close(connector.manualErrSignal)
	}
}

// Set the channel length to something >0. Maybe read the source before using it.
func (connector *Connector) SetChannelLength(l uint) {
	connector.channelLength = l
}

// Start a Kafka Consumer with the specified parameters. Its output will be
// available in the channel returned by ConsumerChannel.
func (connector *Connector) StartConsumer(broker string, topics []string, consumergroup string, offset int64) error {
	var err error
	if !connector.manualErrFlag && connector.manualErrSignal == nil {
		connector.manualErrSignal = make(chan bool)
	}
	brokers := strings.Split(broker, ",")
	consConf := cluster.NewConfig()
	// Enable TLS
	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		log.Println("TLS Error:", err)
		return err
	}
	consConf.Net.TLS.Enable = true
	consConf.Net.TLS.Config = &tls.Config{RootCAs: rootCAs}

	consConf.Net.SASL.Enable = true
	if connector.user == "" && connector.pass == "" {
		log.Println("No Auth information is set. Assuming anonymous auth...")
		connector.SetAuthAnon()
	}
	consConf.Net.SASL.User = connector.user
	consConf.Net.SASL.Password = connector.pass

	// Enable these unconditionally.
	consConf.Consumer.Return.Errors = true
	consConf.Group.Return.Notifications = true
	// The offset only works initially. When reusing a Consumer Group, it's
	// last state will be resumed automatcally (grep MarkOffset)
	consConf.Consumer.Offsets.Initial = offset

	// everything declared and configured, lets go
	log.Printf("Trying to connect to Kafka as SASL/PLAIN user '%s'...", consConf.Net.SASL.User)
	connector.consumer, err = cluster.NewConsumer(brokers, consumergroup, topics, consConf)
	if err != nil {
		return err
	}
	log.Println("Kafka Consumer TLS connection established.")

	// start message handling in background
	connector.consumerChannel = make(chan *flow.FlowMessage, connector.channelLength)
	connector.consumerControlChannel = make(chan ConsumerControlMessage, connector.channelLength)
	go decodeMessages(connector)
	if !connector.manualErrFlag {
		go func() {
			log.Println("Spawned a Consumer Logger, no manual error handling.")
			running := true
			errors := connector.consumer.Errors()
			notifications := connector.consumer.Notifications()
			for running {
				select {
				case msg, ok := <-errors:
					if !ok {
						errors = nil // nil channels are never selected
						log.Println("Kafka Consumer Error: Channel Closed.")
					} else {
						log.Printf("Kafka Consumer Error: %s\n", msg.Error())
					}
				case msg, ok := <-notifications:
					if !ok {
						notifications = nil // nil channels are never selected
						log.Println("Kafka Consumer Notification: Channel Closed.")
					} else {
						log.Printf("Kafka Consumer Notification: %+v\n", msg)
					}
				case _, ok := <-connector.manualErrSignal:
					running = ok
				}
				if errors == nil && notifications == nil {
					log.Println("Consumer Logger: All upstream channels are closed.")
					break
				}
			}
			log.Println("Consumer Logger terminated.")
		}()
	}
	return nil
}

// Start a Kafka Producer with the specified parameters. The channel returned
// by ProducerChannel will be accepting your input.
func (connector *Connector) StartProducer(broker string) error {
	var err error
	if !connector.manualErrFlag && connector.manualErrSignal == nil {
		connector.manualErrSignal = make(chan bool)
	}
	brokers := strings.Split(broker, ",")
	prodConf := sarama.NewConfig()
	// Enable TLS
	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		log.Println("TLS Error:", err)
		return err
	}
	prodConf.Net.TLS.Enable = true
	prodConf.Net.TLS.Config = &tls.Config{RootCAs: rootCAs}

	prodConf.Net.SASL.Enable = true
	if connector.user == "" && connector.pass == "" {
		log.Println("No Auth information is set. Assuming anonymous auth...")
		connector.SetAuthAnon()
	}
	prodConf.Net.SASL.User = connector.user
	prodConf.Net.SASL.Password = connector.pass

	prodConf.Producer.Return.Successes = false // this would block until we've read the ACK
	prodConf.Producer.Return.Errors = true

	connector.producerChannels = make(map[string](chan *flow.FlowMessage))
	// everything declared and configured, lets go
	connector.producer, err = sarama.NewAsyncProducer(brokers, prodConf)
	if err != nil {
		return err
	}
	log.Println("Kafka Producer TLS connection established.")

	// start message handling in background
	if !connector.manualErrFlag {
		go func() {
			log.Println("Spawned a Producer Logger, no manual error handling.")
			running := true
			for running {
				select {
				case msg, ok := <-connector.producer.Errors():
					if !ok {
						running = false
						log.Println("Kafka Producer Error: Channel Closed.")
						continue
					}
					log.Printf("Kafka Producer Error: %s\n", msg.Error())
				case _, ok := <-connector.manualErrSignal:
					running = ok
				}
			}
			log.Println("Producer Logger terminated.")
		}()
	}
	return nil
}

// Close closes the connection to kafka, i.e. Consumer and Producer
func (connector *Connector) Close() {
	log.Println("Kafka Connector Close method called.")
	if connector.consumer != nil {
		connector.consumer.Close()
		log.Println("Kafka Consumer connection closed.")
	}
	if connector.producer != nil {
		connector.producer.Close()
		log.Println("Kafka Producer connection closed.")
	}
}

// Close the Kafka Consumer specifically.
func (connector *Connector) CloseConsumer() {
	log.Println("Kafka Connector CloseConsumer method called.")
	if connector.consumer != nil {
		connector.consumer.Close()
		log.Println("Kafka Consumer connection closed.")
	} else {
		log.Println("WARNING: CloseConsumer called, but no Consumer was initialized.")
	}
}

// Close the Kafka Producer specifically.
func (connector *Connector) CloseProducer() {
	log.Println("Kafka Connector CloseProducer method called.")
	for topic, channel := range connector.producerChannels {
		log.Printf("Kafka Producer: Closed producer channel for topic %s.\n", topic)
		close(channel)
	}
	if connector.producer != nil {
		connector.producer.Close()
		log.Println("Kafka Producer connection closed.")
	} else {
		log.Println("WARNING: CloseProducer called, but no Producer was initialized.")
	}
}

// Return the channel used for receiving Flows from the Kafka Consumer.
// If this channel closes, it means the upstream Kafka Consumer has closed its
// channel previously of the last decoding step. You can restart the Consumer
// by using .StartConsumer() on the same Connector object.
func (connector *Connector) ConsumerChannel() <-chan *flow.FlowMessage {
	return connector.consumerChannel
}

// Return the channel used for handing over Flows to the Kafka Producer.
// If writing to this channel blocks, check the log.
func (connector *Connector) ProducerChannel(topic string) chan *flow.FlowMessage {
	if channel, initialized := connector.producerChannels[topic]; initialized {
		return channel
	}
	connector.producerChannels[topic] = make(chan *flow.FlowMessage, connector.channelLength)
	go encodeMessages(connector.producer, topic, connector.producerChannels[topic])
	return connector.producerChannels[topic]
}

// Consumer Errors relayed directly from the Kafka Cluster.
//
// This will become an exclusive reference only after EnableManualErrorHandling
// has been called.
// IMPORTANT: read EnableManualErrorHandling docs carefully
func (connector *Connector) ConsumerErrors() <-chan error {
	return connector.consumer.Errors()
}

// Consumer Notifications are relayed directly from the Kafka Cluster.
// These include which topics and partitions are read by this instance
// and are sent on every Rebalancing Event.
//
// This will become an exclusive reference only after EnableManualErrorHandling
// has been called.
// IMPORTANT: read EnableManualErrorHandling docs carefully
func (connector *Connector) ConsumerNotifications() <-chan *cluster.Notification {
	return connector.consumer.Notifications()
}

// Producer Errors are relayed directly from the Kafka Cluster.
//
// This will become an exclusive reference only after EnableManualErrorHandling
// has been called.
// IMPORTANT: read EnableManualErrorHandling docs carefully
func (connector *Connector) ProducerErrors() <-chan *sarama.ProducerError {
	return connector.producer.Errors()
}

// GetConsumerControlMessages enables and returns a channel for control messages `ConsumerControlMessage`
func (connector *Connector) GetConsumerControlMessages() <-chan ConsumerControlMessage {
	connector.hasConsumerControlListener = true
	return connector.consumerControlChannel
}

// CancelConsumerControlMessages disables the channel for control messages `ConsumerControlMessage`
func (connector *Connector) CancelConsumerControlMessages() {
	connector.hasConsumerControlListener = false
}
