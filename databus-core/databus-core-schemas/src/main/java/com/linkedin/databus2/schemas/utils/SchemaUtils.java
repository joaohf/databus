package com.linkedin.databus2.schemas.utils;
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

public class SchemaUtils
{
  private SchemaUtils() {}

  public static String toCamelCase(String columnName)
  {
    return toCamelCase(columnName, false);
  }

  public static String toCamelCase(String columnName, boolean initialCap)
  {
    boolean afterUnderscore = false;
    StringBuilder sb = new StringBuilder(columnName.length());
    for(int i=0; i < columnName.length(); i++)
    {
      char ch = columnName.charAt(i);
      if(ch == '_')
      {
        afterUnderscore = true;
      }
      else if(afterUnderscore)
      {
        sb.append(Character.toUpperCase(ch));
        afterUnderscore = false;
      }
      else
      {
        sb.append(Character.toLowerCase(ch));
        afterUnderscore = false;
      }
    }

    if(initialCap && sb.length() > 0)
    {
      sb.replace(0, 1, sb.substring(0,1).toUpperCase());
    }

    return sb.toString();
  }

}
