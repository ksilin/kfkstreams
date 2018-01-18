package com

package object example {

  import java.util.Properties

  implicit class MapPropConverter(m: Map[String, AnyRef]) {
    def toProps: Properties = m.foldLeft(new Properties) {
      case (props, (k, v)) => props.put(k, v); props
    }
  }
}
