# Go Microphage

A war has been raging underground for a long time now. The Microbes and Immune have been engaged in a bloody battle for control of the Human System. Choose your side, taste victory!

## Usage

1. supply the files aiven.env and concluent.env. See sample-env.
2. `go run *.go`

## Notes
- Aiven only offers connection over TLS. So, you need to specify at leat the CA Cert in the connection parameters. [ Aiven Kafka services are offered only over encrypted TLS connections](https://bit.ly/2vLSFgW)

## Best Practices

- When doing concurency with Goroutines, explicitly define Wait Groups and add your goroutines to them, then let your main function wait on this waitgroup. Otherwise, you might spend time wondering why your goroutines are not executing...because your main quits and closes all goroutines it spurn.