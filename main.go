package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/common/log"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

func newKafkaWriter() *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"kafka-prod-project-3a94.aivencloud.com:19077"},
		Topic:   "microphage.toxin.created",
	})
}

func main() {
	// Ensure all required Envars are set
	brokersURL, found := os.LookupEnv("KAFKA_BROKERS_URLS")
	if !found {
		_ = errors.New("environment variable KAFKA_BROKERS_URLS required but not found")
		return
	}

	cacertfile, found := os.LookupEnv("KAFKA_CACERT_FILE")
	if !found {
		_ = errors.New("environment variable KAFKA_CACERT_FILE required but not found")
		return
	}

	topic, found := os.LookupEnv("KAFKA_TOPIC")
	if !found {
		_ = errors.New("environment variable KAFKA_TOPIC required but not found")
		return
	}

	clientUsername, found := os.LookupEnv("KAFKA_CLIENT_USERNAME")
	if !found {
		_ = errors.New("environment variable KAFKA_CLIENT_USERNAME required but not found")
		return
	}

	clientPassword, found := os.LookupEnv("KAFKA_CLIENT_PASSWORD")
	if !found {
		_ = errors.New("environment variable KAFKA_CLIENT_PASSWORD required but not found")
		return
	}

	// Setup connection to Kafka

	saslPlain := plain.Mechanism{
		Username: clientUsername,
		Password: clientPassword,
	}

	// Confluent brokers use a public CA cert, while Aiven's use a private CA cert.
	// Hence if we are verifying the broker certs
	// 		For Confluent: the client should use our host trusted CAs.
	// 		For Aiven: the client should specifically trust the Aiven CA.
	var caCert []byte
	var caCertPool *x509.CertPool

	if cacertfile != "" {
		cert, err := ioutil.ReadFile(cacertfile)

		if err != nil {
			log.Fatal(err)
		}

		caCert = cert
		caCertPool = x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
	}

	dialer := &kafka.Dialer{
		Timeout:       5 * time.Second,
		DualStack:     true,
		SASLMechanism: saslPlain,
		TLS: &tls.Config{
			InsecureSkipVerify: false,
			RootCAs:            caCertPool,
		},
	}

	// Make a topic writer
	w := kafka.NewWriter(kafka.WriterConfig{
		Dialer:  dialer,
		Brokers: strings.Split(brokersURL, " "),
		Topic:   topic,
	})

	// Make a topic reader
	r := kafka.NewReader(kafka.ReaderConfig{
		Dialer:  dialer,
		Brokers: strings.Split(brokersURL, " "),
		Topic:   topic,
	})

	// Run a producer and consumer concurrently. Use a waitgroup to wait for both.
	var wg sync.WaitGroup

	wg.Add(2)
	go produceMessage(w, 5*time.Second, &wg)
	go consumeMessage(r, &wg)

	wg.Wait()
}

func produceMessage(w *kafka.Writer, interval time.Duration, wg *sync.WaitGroup) (err error) {
	defer w.Close()
	defer wg.Done()

	for {
		payload := uuid.New().String()

		err = w.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte("token"),
			Value: []byte(payload),
		})

		if err != nil {
			log.Errorf("Failed to produce message %s", err.Error())
			return
		}

		log.Infof("Producer produced payload <%s>\n", payload)

		time.Sleep(interval)
	}
}

func consumeMessage(r *kafka.Reader, wg *sync.WaitGroup) (err error) {
	defer r.Close()
	defer wg.Done()

	for {
		msg, err := r.ReadMessage(context.Background())

		if err != nil {
			log.Errorf("failed to read message from Kafka topic %s", err.Error())
			return err
		}

		log.Infof("Consumer received message: <partition: %d, offset: %d, key: %s, value: %s>\n", msg.Partition, msg.Offset, msg.Key, msg.Value)
	}
}
