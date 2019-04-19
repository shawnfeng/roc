// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rocserv

import (
	"fmt"
	"net"
	"net/http"

	"github.com/julienschmidt/httprouter"

	"git.apache.org/thrift.git/lib/go/thrift"

	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/snetutil"
)

func powerHttp(addr string, router *httprouter.Router) (string, error) {
	fun := "powerHttp -->"

	paddr, err := snetutil.GetListenAddr(addr)
	if err != nil {
		return "", err
	}

	slog.Infof("%s config addr[%s]", fun, paddr)

	tcpAddr, err := net.ResolveTCPAddr("tcp", paddr)
	if err != nil {
		return "", err
	}

	netListen, err := net.Listen(tcpAddr.Network(), tcpAddr.String())
	if err != nil {
		return "", err
	}

	laddr, err := snetutil.GetServAddr(netListen.Addr())
	if err != nil {
		netListen.Close()
		return "", err
	}

	slog.Infof("%s listen addr[%s]", fun, laddr)

	go func() {
		err := http.Serve(netListen, router)
		if err != nil {
			slog.Panicf("%s laddr[%s]", fun, laddr)
		}
	}()

	return laddr, nil
}

func powerThrift(addr string, processor thrift.TProcessor) (string, error) {
	fun := "powerThrift -->"

	paddr, err := snetutil.GetListenAddr(addr)
	if err != nil {
		return "", err
	}

	slog.Infof("%s config addr[%s]", fun, paddr)

	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	//protocolFactory := thrift.NewTCompactProtocolFactory()

	serverTransport, err := thrift.NewTServerSocket(paddr)
	if err != nil {
		return "", err
	}

	server := thrift.NewTSimpleServer4(processor, serverTransport, transportFactory, protocolFactory)

	// Listen后就可以拿到端口了
	//err = server.Listen()
	err = serverTransport.Listen()
	if err != nil {
		return "", err
	}

	laddr, err := snetutil.GetServAddr(serverTransport.Addr())
	if err != nil {
		return "", err
	}

	slog.Infof("%s listen addr[%s]", fun, laddr)

	go func() {
		err := server.Serve()
		if err != nil {
			slog.Panicf("%s laddr[%s]", fun, laddr)
		}
	}()

	return laddr, nil

}

//启动grpc ，并返回端口信息
func powerGrpc(addr string, server *GrpcServer) (string, error) {
	fun := "powerGrpc -->"
	paddr, err := snetutil.GetListenAddr(addr)
	if err != nil {
		return "", err
	}
	slog.Infof("%s config addr[%s]", fun, paddr)
	lis, err := net.Listen("tcp", paddr)
	if err != nil {
		return "", fmt.Errorf("grpc tcp Listen err:%v", err)
	}
	laddr, err := snetutil.GetServAddr(lis.Addr())
	if err != nil {
		return "", fmt.Errorf(" GetServAddr err:%v", err)
	}
	slog.Infof("%s listen grpc addr[%s]", fun, laddr)
	go func() {
		if err := server.Server.Serve(lis); err != nil {
			slog.Panicf("%s grpc laddr[%s]", fun, laddr)
		}
	}()
	return laddr, nil
}
