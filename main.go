/*
 * Copyright 2020-2021 the original author(https://github.com/wj596)
 *
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"

	"github.com/gin-gonic/gin"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"

	"go-mysql-transfer/global"
	"go-mysql-transfer/metrics"
	"go-mysql-transfer/service"
	"go-mysql-transfer/storage"
	"go-mysql-transfer/util/stringutil"
	"go-mysql-transfer/web"
)

var (
	helpFlag          bool
	cfgPath           string
	stockFlag         bool
	positionFlag      bool
	resetPositionflag bool
	statusFlag        bool
)

func init() {
	flag.BoolVar(&helpFlag, "help", false, "this help")
	flag.StringVar(&cfgPath, "config", "/etc/vnet-mysql-transfer/app.yml", "application config file")
	flag.BoolVar(&stockFlag, "stock", false, "stock data import")
	flag.BoolVar(&positionFlag, "position", false, "set dump position")
	flag.BoolVar(&resetPositionflag, "resetPosition", false, "set dump position")
	flag.BoolVar(&statusFlag, "status", false, "display application status")
	flag.Usage = usage
}

func main() {
	flag.Parse()
	if helpFlag {
		flag.Usage()
		return
	}

	// 初始化global
	err := global.Initialize(cfgPath)
	if err != nil {
		println(errors.ErrorStack(err))
		return
	}

	if stockFlag {
		doStock()
	}

	// 初始化Storage
	err = storage.Initialize()
	if err != nil {
		println(errors.ErrorStack(err))
		return
	}

	if statusFlag {
		doStatus()
		return
	}
	if resetPositionflag {
		doResetPosition()
		return
	}

	if positionFlag {
		doPosition()
		return
	}
	err = service.Initialize()
	if err != nil {
		println(errors.ErrorStack(err))
		return
	}
	if err := metrics.Initialize(); err != nil {
		println(errors.ErrorStack(err))
		return
	}

	if err := web.Start(); err != nil {
		println(errors.ErrorStack(err))
		return
	}
	service.StartUp() // start application
	log.Println("service StartUp")

	go startHealthCheckServer(context.Background(), global.Cfg().HcPort)
	log.Println("healthcheck StartUp")
	<-context.Background().Done()

	defer web.Close()
	defer service.Close()
	defer storage.Close()
}

func exit(ch <-chan os.Signal, graceCancel context.CancelFunc) {
	// first close hc port
	log.Println("healthcheck stop")
	graceCancel()
}

func startHealthCheckServer(ctx context.Context, port int) {
	endpoint := fmt.Sprintf(":%d", port)
	r := gin.New()
	r.Use(gin.Recovery())
	r.GET("/healthy", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"response": "OK"})
	})
	server := http.Server{Addr: endpoint, Handler: r}
	go func() {
		err := server.ListenAndServe()
		if err != nil {
			log.Println("HealthCheck Server failed")
		}
	}()

	<-ctx.Done()
	_ = server.Shutdown(context.Background())
	log.Println("Health check listener stopped")
}

func doStock() {
	stock := service.NewStockService()
	if err := stock.Run(); err != nil {
		println(errors.ErrorStack(err))
	}
	stock.Close()
}

func doStatus() {
	ps := storage.NewPositionStorage()
	pos, _ := ps.Get()
	fmt.Printf("The current dump position is : %s %d \n", pos.Name, pos.Pos)
}

func doPosition() {
	others := flag.Args()
	if len(others) != 2 {
		println("error: please input the binlog's File and Position")
		return
	}
	f := others[0]
	p := others[1]

	matched, _ := regexp.MatchString(".+\\.\\d+$", f)
	if !matched {
		println("error: The parameter File must be like: mysql-bin.000001")
		return
	}

	pp, err := stringutil.ToUint32(p)
	if nil != err {
		println("error: The parameter Position must be number")
		return
	}
	ps := storage.NewPositionStorage()
	pos := mysql.Position{
		Name: f,
		Pos:  pp,
	}
	ps.Save(pos)
	fmt.Printf("The current dump position is : %s %d \n", f, pp)
}

func doResetPosition() {
	ps := storage.NewPositionStorage()
	ps.Reset()
}

func usage() {
	fmt.Fprintf(os.Stderr, `version: 1.0.0
Usage: transfer [-c filename] [-s stock]

Options:
`)
	flag.PrintDefaults()
}
