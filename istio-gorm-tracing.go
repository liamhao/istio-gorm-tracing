package istiogormtracing

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/opentracing/opentracing-go"
	opentracinglog "github.com/opentracing/opentracing-go/log"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	jaegerlog "github.com/uber/jaeger-client-go/log"
	"github.com/uber/jaeger-client-go/zipkin"
	"gorm.io/gorm"
)

type IstioGormTracing struct {
	ServiceName       string
	CollectorEndpoint string
}

var (
	// 保留 Istio 发送请求时的 header 信息(x-b3-traceid|x-b3-parentspanid|x-b3-spanid|x-b3-sampled)
	H http.Header
	// 注册插件
	_ gorm.Plugin = &IstioGormTracing{}
)

const (
	spankey = "istio-gorm-tracing"

	// 自定义事件名称
	_eventBeforeCreate = "istio-gorm-tracing-event:before_create"
	_eventAfterCreate  = "istio-gorm-tracing-event:after_create"
	_eventBeforeUpdate = "istio-gorm-tracing-event:before_update"
	_eventAfterUpdate  = "istio-gorm-tracing-event:after_update"
	_eventBeforeQuery  = "istio-gorm-tracing-event:before_query"
	_eventAfterQuery   = "istio-gorm-tracing-event:after_query"
	_eventBeforeDelete = "istio-gorm-tracing-event:before_delete"
	_eventAfterDelete  = "istio-gorm-tracing-event:after_delete"
	_eventBeforeRow    = "istio-gorm-tracing-event:before_row"
	_eventAfterRow     = "istio-gorm-tracing-event:after_row"
	_eventBeforeRaw    = "istio-gorm-tracing-event:before_raw"
	_eventAfterRaw     = "istio-gorm-tracing-event:after_raw"

	// 自定义 span 的操作名称
	_opCreate = "create"
	_opUpdate = "update"
	_opQuery  = "query"
	_opDelete = "delete"
	_opRow    = "row"
	_opRaw    = "raw"
)

// 开箱即用，svcName: 此项目的微服务名称，collectorEndpoint: jaeger 收集器的地址(如:http://127.0.0.1:14268/api/traces)
func NewDefault(svcName, collectorEndpoint string) gorm.Plugin {
	i := &IstioGormTracing{
		ServiceName:       svcName,
		CollectorEndpoint: collectorEndpoint,
	}
	i.bootTracerBasedJaeger()
	return i
}

// 实现 gorm 插件所需方法
func (p *IstioGormTracing) Name() string {
	return "IstioGormTracing"
}

// 实现 gorm 插件所需方法
func (p *IstioGormTracing) Initialize(db *gorm.DB) (err error) {
	// 在 gorm 中注册各种回调事件
	for _, e := range []error{
		db.Callback().Create().Before("gorm:create").Register(_eventBeforeCreate, beforeCreate),
		db.Callback().Create().After("gorm:create").Register(_eventAfterCreate, after),
		db.Callback().Update().Before("gorm:update").Register(_eventBeforeUpdate, beforeUpdate),
		db.Callback().Update().After("gorm:update").Register(_eventAfterUpdate, after),
		db.Callback().Query().Before("gorm:query").Register(_eventBeforeQuery, beforeQuery),
		db.Callback().Query().After("gorm:query").Register(_eventAfterQuery, after),
		db.Callback().Delete().Before("gorm:delete").Register(_eventBeforeDelete, beforeDelete),
		db.Callback().Delete().After("gorm:delete").Register(_eventAfterDelete, after),
		db.Callback().Row().Before("gorm:row").Register(_eventBeforeRow, beforeRow),
		db.Callback().Row().After("gorm:row").Register(_eventAfterRow, after),
		db.Callback().Raw().Before("gorm:raw").Register(_eventBeforeRaw, beforeRaw),
		db.Callback().Raw().After("gorm:raw").Register(_eventAfterRaw, after),
	} {
		if e != nil {
			return e
		}
	}
	return
}

// 注册各种前置事件时，对应的事件方法
func _injectBefore(db *gorm.DB, op string) {

	if db == nil {
		return
	}

	if db.Statement == nil || db.Statement.Context == nil {
		db.Logger.Error(context.TODO(), "未定义 db.Statement 或 db.Statement.Context")
		return
	}

	// 这里是关键，通过 istio 传过来的 header 解析出父 span，如果没有，则会创建新的根 span
	zipkinPropagator := zipkin.NewZipkinB3HTTPHeaderPropagator()
	spanCtx, _ := zipkinPropagator.Extract(opentracing.HTTPHeadersCarrier(H))
	span, _ := opentracing.StartSpanFromContext(db.Statement.Context, op, opentracing.ChildOf(spanCtx))
	db.InstanceSet(spankey, span)
}

// 注册后置事件时，对应的事件方法
func after(db *gorm.DB) {

	if db == nil {
		return
	}

	if db.Statement == nil || db.Statement.Context == nil {
		db.Logger.Error(context.TODO(), "未定义 db.Statement 或 db.Statement.Context")
		return
	}

	_span, isExist := db.InstanceGet(spankey)
	if !isExist || _span == nil {
		return
	}

	// 断言，进行类型转换
	span, ok := _span.(opentracing.Span)
	if !ok || span == nil {
		return
	}
	defer span.Finish()

	// 记录error
	if db.Error != nil {
		span.LogFields(opentracinglog.Error(db.Error))
	}

	// 记录其他内容
	span.LogFields(opentracinglog.String("sql", db.Dialector.Explain(db.Statement.SQL.String(), db.Statement.Vars...)))
	span.LogFields(opentracinglog.String("table", db.Statement.Table))
	span.LogFields(opentracinglog.String("query", db.Statement.SQL.String()))
	span.LogFields(opentracinglog.String("bindings", db.Statement.SQL.String()))
}

func beforeCreate(db *gorm.DB) {
	_injectBefore(db, _opCreate)
}

func beforeUpdate(db *gorm.DB) {
	_injectBefore(db, _opUpdate)
}

func beforeQuery(db *gorm.DB) {
	_injectBefore(db, _opQuery)
}

func beforeDelete(db *gorm.DB) {
	_injectBefore(db, _opDelete)
}

func beforeRow(db *gorm.DB) {
	_injectBefore(db, _opRow)
}

func beforeRaw(db *gorm.DB) {
	_injectBefore(db, _opRaw)
}

// 默认初始化一个 jaeger tracer
func (i IstioGormTracing) bootTracerBasedJaeger() {
	// 基础配置
	tracer, _, err := config.Configuration{
		Sampler: &config.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1,
		},
		ServiceName: i.ServiceName,
		Reporter: &config.ReporterConfig{
			LogSpans:          true,
			CollectorEndpoint: i.CollectorEndpoint,
		},
	}.NewTracer(
		config.Logger(jaegerlog.StdLogger),
	)

	if err != nil {
		log.Printf("jaeger tracer 插件初始化失败, 错误原因: %v", err)
		os.Exit(1)
	}

	// 设为全局使用的 tracer
	opentracing.SetGlobalTracer(tracer)
}
