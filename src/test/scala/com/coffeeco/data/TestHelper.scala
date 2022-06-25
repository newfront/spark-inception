package com.coffeeco.data

object TestHelper {
  def fullPath(filePath: String): String = new java.io.File(filePath).getAbsolutePath
}
