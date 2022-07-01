# 微服务追踪 gorm 插件

在`kubernetes`上使用`Istio`管控微服务后，微服务之间的调用会自动上传到`Jaeger`的收集器。但只能追踪到服务层，这是我不能接受的，我希望能进一步追踪到服务中的所有`MySQL`查询，记录每个 sql 的耗时，所以，我简单写了这个插件。

# 特性

### 支持`Istio`

在`Istio`管控下的容器请求之间，会自动携带`x-b3-traceid`、`x-b3-parentspanid`、`x-b3-spanid`、`x-b3-sampled`等请求头，这些请求头都是与`zipkin`对齐的。此插件中会根据传递进来的请求头信息，自动解析出父`span`，并绑定上下服务之间的调用关系。

### 记录SQL信息

每次查询都会记录下执行的SQL语句以及执行耗时等信息，作为后期微服务追踪的依据。

# 使用

```golang
package main

import (
    istiogormtracing "github.com/liamhao/istio-gorm-tracing"
    "log"
    "fmt"
    "github.com/gin-gonic/gin"
    "gorm.io/driver/mysql"
    "gorm.io/gorm"
)

func main() {

    router := gin.Default()

    dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local", "dbuser", "dbpswd", "dbhost", 3306, "dbname")
    gormDb, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
    if err != nil {
        log.Println("mysql连接出现了问题：", err.Error())
    } else {
        log.Println("mysql连接成功：", dsn)
    }

    // 这一步很关键，一定要加上，为了启用我们的插件
    gormDb.Use(istiogormtracing.NewDefault(
        // 你的微服务名称
        "istiogormtracing-service",
        // 你的 Jaeger 收集器地址
        "http://127.0.0.1:14268/api/traces",
    ))

    router.GET("/", func(c *gin.Context) {

        // 这一步很关键，一定要加上，为了SQL能与上下游服务做关联
        istiogormtracing.H = c.Request.Header

        list := []map[string]interface{}{}
        gormDb.Debug().Table("users").Where("name = 'xiaoming'").Find(&list)

        c.JSON(http.StatusOK, map[string]interface{}{
            "istiogormtracing": "ok",
        })
    })

    router.Run(":7000")
}
```

然后即可在`Jaeger`面板中看到我们记录的SQL了。

# 效果图

SQL的追踪正确插入到微服务的调用链之间

![图片](https://cdn.learnku.com/uploads/images/202207/01/41543/3IP5HGllbb.png)

详细记录了SQL的执行内容和消耗时间

![图片](https://cdn.learnku.com/uploads/images/202207/01/41543/iOMmQ4bkPI.png)