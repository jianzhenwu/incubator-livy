/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.livy.server.ui

import java.io.{ByteArrayOutputStream, IOException}
import java.lang.management.ManagementFactory
import java.lang.reflect.Array
import javax.management.{AttributeNotFoundException, InstanceNotFoundException, IntrospectionException, MalformedObjectNameException, MBeanAttributeInfo, MBeanException, MBeanInfo, ObjectName, ReflectionException, RuntimeErrorException, RuntimeMBeanException}
import javax.management.openmbean.{CompositeData, TabularData}

import scala.collection.JavaConverters._

import com.fasterxml.jackson.core.{JsonFactory, JsonGenerator}
import org.scalatra.ScalatraServlet

import org.apache.livy.Logging

class JmxJsonServlet extends ScalatraServlet with Logging {

  before() {
    contentType = "application/json"
  }

  get("/") {
    doGet(params.get("qry"), params.get("get"))
  }

  private val mBeanServer = ManagementFactory.getPlatformMBeanServer
  private val jsonFactory = new JsonFactory
  private val ERROR_MESSAGE = "Bad Request occur"

  def doGet(qry: Option[String], getMethod: Option[String]): String = {
    val baos = new ByteArrayOutputStream(1024)
    var jg: JsonGenerator = null
    try {
      jg = jsonFactory.createJsonGenerator(baos);
      jg.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
      jg.useDefaultPrettyPrinter
      jg.writeStartObject()
      if (getMethod.isDefined) {
        val splitStrings = getMethod.get.split("::")
        if (splitStrings.length != 2) {
          jg.writeStringField("result", "ERROR")
          jg.writeStringField("message", "query format is not as expected.")
          jg.writeEndObject()
          jg.flush()
          return baos.toString("UTF-8")
        }
        listBeans(jg, new ObjectName(splitStrings(0)), splitStrings(1))
      }
      if (qry.isDefined) {
        listBeans(jg, new ObjectName(qry.get), null)
      } else {
        listBeans(jg, new ObjectName("*:*"), null)
      }
      jg.writeEndObject()
      jg.flush()
      baos.toString("UTF-8")
    } catch {
      case e: IOException =>
        error("Caught an exception while processing JMX request", e)
        ERROR_MESSAGE
      case e: MalformedObjectNameException =>
        error("Caught an exception while processing JMX request", e)
        ERROR_MESSAGE
    } finally {
      if (jg != null) {
        jg.close()
      }
      baos.close()
    }
  }

  @throws[IOException]
  private def listBeans(jg: JsonGenerator, qry: ObjectName, attribute: String): Unit = {
    val names = mBeanServer.queryNames(qry, null)
    jg.writeArrayFieldStart("beans")
    val it = names.iterator()
    while (it.hasNext) {
      val oname: ObjectName = it.next
      var minfo: MBeanInfo = null
      var code: String = ""
      var isError = false
      var attributeinfo: AnyRef = null
      try {
        minfo = mBeanServer.getMBeanInfo(oname)
        code = minfo.getClassName
        var prs: String = ""
        try {
          if ("org.apache.commons.modeler.BaseModelMBean" == code) {
            prs = "modelerType"
            code = mBeanServer.getAttribute(oname, prs).asInstanceOf[String]
          }
          if (attribute != null) {
            prs = attribute
            attributeinfo = mBeanServer.getAttribute(oname, prs)
          }
        } catch {
          case e: AttributeNotFoundException =>
            // If the modelerType attribute was not found, the class name is used
            // instead.
            error("getting attribute " + prs + " of " + oname + " threw an exception", e)
          case e: MBeanException =>
            // The code inside the attribute getter threw an exception so log it,
            // and fall back on the class name
            error("getting attribute " + prs + " of " + oname + " threw an exception", e)
          case e: RuntimeException =>
            // For some reason even with an MBeanException available to them
            // Runtime exceptions can still find their way through, so treat them
            // the same as MBeanException
            error("getting attribute " + prs + " of " + oname + " threw an exception", e)
          case e: ReflectionException =>
            // This happens when the code inside the JMX bean (setter?? from the
            // java docs) threw an exception, so log it and fall back on the
            // class name
            error("getting attribute " + prs + " of " + oname + " threw an exception", e)
        }
      } catch {
        case e: InstanceNotFoundException =>
          // Ignored for some reason the bean was not found so don't output it
          isError = true
        case e: IntrospectionException =>
          // This is an internal error, something odd happened with reflection so
          // log it and don't output the bean.
          error("Problem while trying to process JMX query: " +
            qry + " with MBean " + oname, e)
          isError = true

        case e: ReflectionException =>
          // This happens when the code inside the JMX bean threw an exception, so
          error("Problem while trying to process JMX query: " +
            qry + " with MBean " + oname, e)
          isError = true
        case e: Exception =>
          isError = true
      }
      if (!isError) {
        jg.writeStartObject()
        jg.writeStringField("name", oname.toString)
        jg.writeStringField("modelerType", code)
        if ((attribute != null) && (attributeinfo == null)) {
          jg.writeStringField("result", "ERROR")
          jg.writeStringField("message", "No attribute with name "
            + attribute + " was found.")
          jg.writeEndObject()
          jg.writeEndArray()
          return
        }

        if (attribute != null) {
          writeAttribute(jg, attribute, attributeinfo)
        }
        else {
          minfo.getAttributes.foreach(writeAttribute(jg, oname, _))
        }
        jg.writeEndObject()
      }
    }
    jg.writeEndArray()
  }

  @throws[IOException]
  private def writeAttribute(
      jg: JsonGenerator,
      oname: ObjectName,
      attr: MBeanAttributeInfo): Unit = {
    if (!attr.isReadable) {
      return
    }
    val attName = attr.getName
    if ("modelerType" == attName) {
      return
    }
    if (attName.indexOf("=") >= 0 || attName.indexOf(":") >= 0 || attName.indexOf(" ") >= 0) {
      return
    }
    try {
      val value = mBeanServer.getAttribute(oname, attName)
      writeAttribute(jg, attName, value)
    } catch {
      case e: RuntimeMBeanException =>
        // UnsupportedOperationExceptions happen in the normal course of business,
        // so no need to log them as errors all the time.
        if (e.getCause.isInstanceOf[UnsupportedOperationException]) {
          debug("getting attribute " + attName + " of " + oname + " threw an exception", e)
        }
        else {
          error("getting attribute " + attName + " of " + oname + " threw an exception", e)
        }
      case e: RuntimeErrorException =>
        // RuntimeErrorException happens when an unexpected failure occurs in getAttribute
        // for example https://issues.apache.org/jira/browse/DAEMON-120
        debug("getting attribute " + attName + " of " + oname + " threw an exception", e)
      case e: AttributeNotFoundException =>
      // Ignored the attribute was not found, which should never happen because the bean
      // just told us that it has this attribute, but if this happens just don't output
      // the attribute.
      case e: MBeanException =>
        // The code inside the attribute getter threw an exception so log it, and
        // skip outputting the attribute
        error("getting attribute " + attName + " of " + oname + " threw an exception", e)
      case e: RuntimeException =>
        // For some reason even with an MBeanException available to them Runtime exceptions
        // can still find their way through, so treat them the same as MBeanException
        error("getting attribute " + attName + " of " + oname + " threw an exception", e)
      case e: ReflectionException =>
        // This happens when the code inside the JMX bean (setter?? from the java docs)
        // threw an exception, so log it and skip outputting the attribute
        error("getting attribute " + attName + " of " + oname + " threw an exception", e)
      case e: InstanceNotFoundException =>
      // Ignored the mbean itself was not found, which should never happen because we
      // just accessed it (perhaps something unregistered in-between) but if this
      // happens just don't output the attribute.
    }
  }

  @throws[IOException]
  private def writeAttribute(jg: JsonGenerator, attName: String, value: Any): Unit = {
    jg.writeFieldName(attName)
    writeObject(jg, value)
  }

  @throws[IOException]
  private def writeObject(jg: JsonGenerator, value: Any): Unit = {
    if (value == null) {
      jg.writeNull()
    }
    else {
      val c = value.getClass
      if (c.isArray) {
        jg.writeStartArray()
        val len = Array.getLength(value)
        for (j <- 0 until len) {
          val item = Array.get(value, j)
          writeObject(jg, item)
        }
        jg.writeEndArray()
      }
      else {
        value match {
          case n: Number =>
            jg.writeNumber(n.toString)
          case b: Boolean =>
            jg.writeBoolean(b)
          case cds: CompositeData =>
            val keys = cds.getCompositeType.keySet().asScala
            jg.writeStartObject()
            keys.foreach((x) => writeAttribute(jg, x, cds.get(x)))
            jg.writeEndObject()
          case tds: TabularData =>
            jg.writeStartArray()
            for (entry <- tds.values.asScala) {
              writeObject(jg, entry)
            }
            jg.writeEndArray()
          case _ => jg.writeString(value.toString)
        }
      }
    }
  }
}

