package client

// config holds the resolved Client configuration. Defaults are set
// in newConfig; Options mutate it.
type config struct{}

func newConfig() config {
	return config{}
}

// Option mutates a Client's configuration during Dial. New options
// can be added without breaking the Dial signature.
type Option func(*config)
