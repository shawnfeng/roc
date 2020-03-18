package conf

type ConfigerType int

const (
	// ConfigerTypeApollo ...
	ConfigerTypeApollo ConfigerType = iota
	// ConfigerTypeEtcd ...
	ConfigerTypeEtcd
)
