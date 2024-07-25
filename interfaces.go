package main

type Task interface {
	Name() string
	File() string
	Execute()
}

type Closable interface {
	Close() error
}
