package rocserv


type Processor interface {
	// init
	Init() error
	// interace driver
	Driver() (string, interface{})
}

