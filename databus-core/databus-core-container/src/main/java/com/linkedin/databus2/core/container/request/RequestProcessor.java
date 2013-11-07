package com.linkedin.databus2.core.container.request;
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


import java.io.IOException;
import java.util.concurrent.ExecutorService;

import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.request.DatabusRequest;

/**
 * A processor for {@link DatabusRequest} commands. 
 * @author cbotev
 *
 */
public interface RequestProcessor {

  /**
   * Processes the specified databus request
   * @param  request
   * @return the buffer with the response
   * @throws IOException
   */
   DatabusRequest process(DatabusRequest request) throws IOException, RequestProcessingException, DatabusException; 

  /**
   * Obtains the executor service to use for scheduling command for this processor
   * @return	the service
   */
  ExecutorService getExecutorService();
}
