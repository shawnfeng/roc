package rocserv

import (
	"log"
	"testing"
)

func TestIt(t *testing.T) {
    etcds := []string{"http://127.0.0.1:20002"}
	skey := "7e07d3e6-2737-43ac-86fa-157bc1bb8943a"
	//skey = "beauty"
	var sb ServBase
	var err error
	sb, err = NewServBaseV2(etcds, "niubi/fuck", skey)

	if err != nil {
		t.Errorf("create err:%s", err)
		return
	}

	log.Println(sb)

	sfid, err := sb.GenSnowFlakeId()
	if err != nil {
		t.Errorf("snow id err:%s", err)
		return
	}

	log.Println(sfid)


	log.Println(sb.GenUuid())
	log.Println(sb.GenUuidMd5())
	log.Println(sb.GenUuidSha1())


	dr := sb.Dbrouter()
	log.Println(dr)


	type TConf2 struct {
		Uname   string
		Passwd string
		Fuck int
		Girl int64

		// 不是指针的、是指针的，指针为空的或者不为空的
		Ts *struct {
			AAA string
			BBB uint8

			CCC bool

			LLL []int `sep:"," sconf:"lll"`
			M map[string][]string
		}

		Ts1 *string

		Sm map[string]struct {
			Ee string
			Ff string
		}
	}

	var svconf TConf2


	err = sb.ServConfig(&svconf)
	if err != nil {
		t.Errorf("serv config err:%s", err)
		return
	}

	log.Println(svconf)

}


