package rocserv

import (
	"log"
	"testing"
)

func TestIt(t *testing.T) {
    etcds := []string{"http://127.0.0.1:20002"}
	skey := "7e07d3e6-2737-43ac-86fa-157bc1bb89432"
	//skey = "beauty"
	sb, err := NewServBaseV2(etcds, "/disp/service/niubi", "fuck", skey)

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


}


