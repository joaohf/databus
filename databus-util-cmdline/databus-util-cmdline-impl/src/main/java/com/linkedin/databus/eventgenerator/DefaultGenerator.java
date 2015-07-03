package com.linkedin.databus.eventgenerator;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


public class DefaultGenerator implements DefaultDataGenerator
{

  private String stringValue;
  private boolean booleanValue;
  private byte[] bytesValue;
  private int intValue;
  private long longValue;
  private double doubleValue;
  private float floatValue;

  public DefaultGenerator() {
  }

  public void setBoolean(boolean value)
  {
    booleanValue = value;
  }

  public void setBytes(byte[] value)
  {
    bytesValue = value;
  }

  public void setInt(int value)
  {
    intValue = value;
  }

  public void setLong(long value)
  {
    longValue = value;
  }

  public void setDouble(double value)
  {
    doubleValue = value;
  }

  public void setFloat(float value)
  {
    floatValue = value;
  }

  public void setString(String value)
  {
    stringValue = value;
  }

  public int getNextInt()
  {
    return intValue;
  }

  public int getNextInt(int min, int max)
  {
    return getNextInt();
  }

  public String getNextString()
  {
    return stringValue;
  }

  public String getNextString(int min, int max)
  {
    return getNextString();
  }

  public double getNextDouble()
  {
    return doubleValue;
  }

  public float getNextFloat()
  {
    return floatValue;
  }

  public long getNextLong()
  {
    return longValue;
  }

  public boolean getNextBoolean()
  {
    return booleanValue;
  }

  public  byte[] getNextBytes(int maxBytesLength)
  {
    return bytesValue;
  }
}
