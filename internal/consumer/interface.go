package consumer

type Consumer interface {
	Consume() error
}
