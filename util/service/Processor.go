package rocserv


type Processor interface {
	// init
	Init(sb ServBase) error
	// interace driver
	Driver() (string, interface{})
}

