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


import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;


/*
 * The class is a factory which returns an instance based on which random data can be written to the record
 */
public abstract class SchemaFiller {

  protected Field field;
  protected DefaultDataGenerator dataGenerator;

  /*
   * Constructor to create an schema filler instance of the given field.
   * @param field The type of field to create an filler for.
   */
  public SchemaFiller(Field field)
  {
    this.field = field;
    this.dataGenerator = new DefaultRandomGenerator();

  }
  
  /*
   * Constructor to create an schema filler instance of the given field and also lets you define an random generator
   * @param field The type of field to create an filler for.
   * @param randGen The random generator to be used to generate this field (Implement the interface RandomDataGenerator to use your own).
   */
  public SchemaFiller(Field field, DefaultDataGenerator defaultGenerator)
  {
    this.field = field;
    this.dataGenerator = defaultGenerator;
  }

  /*
   * Factory method to generate random data according to type
   * @param field The field based on which random data is be generated
   * @return SchemaFiller The Schemafiller instance let's you write data to the record based on the field passed to the function.
   */
  public static SchemaFiller createRandomField(Field field) throws UnknownTypeException
  {

    Schema.Type type = field.schema().getType();
    if(type == Schema.Type.ARRAY)
    {
      return new ArrayFieldGenerate(field);
    }
    else if(type == Schema.Type.BOOLEAN)
    {
      return new BooleanFieldGenerate(field);
    }
    else if(type == Schema.Type.BYTES)
    {
      return new BytesFieldGenerate(field);
    }
    else if(type == Schema.Type.DOUBLE)
    {
      return new DoubleFieldGenerate(field);
    }
    else if(type == Schema.Type.ENUM)
    {
      return new EnumFieldGenerate(field);
    }
    else if(type == Schema.Type.FIXED)
    {
      return new FixedLengthFieldGenerate(field);
    }
    else if(type == Schema.Type.FLOAT)
    {
      return new FloatFieldGenerate(field);
    }
    else if(type == Schema.Type.INT)
    {
      return new IntegerFieldGenerate(field);
    }
    else if(type == Schema.Type.LONG)
    {
      return new LongFieldGenerate(field);
    }
    else if(type == Schema.Type.MAP)
    {
      return new MapFieldGenerate(field);
    }
    else if(type == Schema.Type.NULL)
    {
      return new NullFieldGenerate(field);
    }
    else if(type == Schema.Type.RECORD)
    {
      return new RecordFieldGenerate(field);
    }
    else if(type == Schema.Type.STRING)
    {
      return new StringFieldGenerate(field);
    }
    else if(type == Schema.Type.UNION)
    {
      return new UnionFieldGenerate(field);
    }
    else
    {
      throw new UnknownTypeException();
    }
  }

  /*
 * Factory method to generate random data according to type
 * @param field The field based on which random data is be generated
 * @return SchemaFiller The Schemafiller instance let's you write data to the record based on the field passed to the function.
 */
  public static SchemaFiller createCsvField(Field field, String csvField) throws UnknownTypeException
  {

    Schema.Type type = field.schema().getType();
    if(type == Schema.Type.ARRAY)
    {
      throw new UnknownTypeException();
    }
    else if(type == Schema.Type.BOOLEAN)
    {
      boolean booleanValue = Boolean.valueOf(csvField);
      return new BooleanFieldGenerate(field, booleanValue);
    }
    else if(type == Schema.Type.BYTES)
    {
      throw new UnknownTypeException();
    }
    else if(type == Schema.Type.DOUBLE)
    {
      double doubleValue = Double.valueOf(csvField);
      return new DoubleFieldGenerate(field, doubleValue);
    }
    else if(type == Schema.Type.ENUM)
    {
      throw new UnknownTypeException();
    }
    else if(type == Schema.Type.FIXED)
    {
      throw new UnknownTypeException();
    }
    else if(type == Schema.Type.FLOAT)
    {
      float floatValue = Float.valueOf(csvField);
      return new FloatFieldGenerate(field, floatValue);
    }
    else if(type == Schema.Type.INT)
    {
      Integer integerValue = Integer.valueOf(csvField);
      return new IntegerFieldGenerate(field, integerValue);
    }
    else if(type == Schema.Type.LONG)
    {
      long longValue = Long.valueOf(csvField);
      return new LongFieldGenerate(field, longValue);
    }
    else if(type == Schema.Type.MAP)
    {
      throw new UnknownTypeException();
    }
    else if(type == Schema.Type.NULL)
    {
      return new NullFieldGenerate(field);
    }
    else if(type == Schema.Type.RECORD)
    {
      throw new UnknownTypeException();
    }
    else if(type == Schema.Type.STRING)
    {
      return new StringFieldGenerate(field, csvField);
    }
    else if(type == Schema.Type.UNION)
    {
      return new UnionFieldDefaultGenerate(field, csvField);
    }
    else
    {
      throw new UnknownTypeException();
    }
  }

  /*
   * Override to write data
   * @param  record  The Genericrecord to which the data is to be written.
   */
  public abstract void writeToRecord(GenericRecord record) throws UnknownTypeException;

  /*
   * return the random generated object. Use this to fetch the object instead of writing to an record.
   */
  public abstract Object generateRandomObject()  throws UnknownTypeException;

}
