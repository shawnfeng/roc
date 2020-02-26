package rocserv

// BaseConfig 基础配置
type BaseConfig struct {
	Base struct {
		CrossRegisterCenters []string `sep:"," sconf:"crossRegisterCenters"`
	}
}
