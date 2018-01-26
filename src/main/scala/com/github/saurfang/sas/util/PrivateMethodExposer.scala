package com.github.saurfang.sas.util

class PrivateMethodCaller(x: AnyRef, methodName: String) {
  def apply(_args: Any*): Any = {
    val args = _args.map(_.asInstanceOf[AnyRef])
    def _parents: Stream[Class[_]] = Stream(x.getClass) #::: _parents.map(_.getSuperclass)
    val parents = _parents.takeWhile(_ != null).toList
    val methods = parents.flatMap(_.getDeclaredMethods)
    val method = methods.find(_.getName == methodName).getOrElse(throw new IllegalArgumentException("Method " + methodName + " not found"))
    method.setAccessible(true)
    method.invoke(x, args : _*)
  }
}

/**
 *
 * Use this to invoke private methods in [[com.epam.parso.impl.SasFileParser]] so we don't need to modify it
 * Credits to https://gist.github.com/jorgeortiz85/908035
 * Usage:
 *  p(instance)('privateMethod)(arg1, arg2, arg3)
 */
case class PrivateMethodExposer(x: AnyRef) {
  def apply(method: scala.Symbol): PrivateMethodCaller = new PrivateMethodCaller(x, method.name)

  def get[T](member: scala.Symbol): T = {
    def _parents: Stream[Class[_]] = Stream(x.getClass) #::: _parents.map(_.getSuperclass)
    val parents = _parents.takeWhile(_ != null).toList
    val fields = parents.flatMap(_.getDeclaredFields)
    val field = fields.find(_.getName == member.name).getOrElse(throw new IllegalArgumentException("Field " + member.name + " not found"))
    field.setAccessible(true)
    field.get(x).asInstanceOf[T]
  }
}