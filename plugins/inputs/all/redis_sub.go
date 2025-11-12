//go:build !custom || inputs || inputs.redis_sub

package all

import _ "github.com/influxdata/telegraf/plugins/inputs/redis_sub" // register plugin
