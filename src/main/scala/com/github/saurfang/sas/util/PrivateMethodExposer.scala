// Copyright (C) 2018 Forest Fang.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.github.saurfang.sas.util

class PrivateMethodCaller(x: AnyRef, methodName: String) {
  def apply(_args: Any*): Any = {
    val args = _args.map(_.asInstanceOf[AnyRef])

    def collectParents: Stream[Class[_]] = Stream(x.getClass) #::: collectParents.map(_.getSuperclass)

    val parents = collectParents.takeWhile(_ != null).toList
    val methods = parents.flatMap(_.getDeclaredMethods)
    val method = methods.find(_.getName == methodName)
      .getOrElse(throw new IllegalArgumentException("Method " + methodName + " not found"))
    method.setAccessible(true)
    method.invoke(x, args: _*)
  }
}

/**
  *
  * Use this to invoke private methods in [[com.epam.parso.impl.SasFileParser]] so we don't need to modify it
  * Credits to https://gist.github.com/jorgeortiz85/908035
  * Usage:
  * p(instance)('privateMethod)(arg1, arg2, arg3)
  */
case class PrivateMethodExposer(x: AnyRef) {
  def apply(method: scala.Symbol): PrivateMethodCaller = new PrivateMethodCaller(x, method.name)

  def get[T](member: scala.Symbol): T = {
    def collectParents: Stream[Class[_]] = Stream(x.getClass) #::: collectParents.map(_.getSuperclass)

    val parents = collectParents.takeWhile(_ != null).toList
    val fields = parents.flatMap(_.getDeclaredFields)
    val field = fields.find(_.getName == member.name)
      .getOrElse(throw new IllegalArgumentException("Field " + member.name + " not found"))
    field.setAccessible(true)
    field.get(x).asInstanceOf[T]
  }
}
