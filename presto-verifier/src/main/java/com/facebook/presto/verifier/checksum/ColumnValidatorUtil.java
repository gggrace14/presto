/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.verifier.checksum;

import com.facebook.presto.verifier.framework.Column;

public class ColumnValidatorUtil
{
    private ColumnValidatorUtil() {}

    public static boolean isDoubleAsStringColumn(Column column, ChecksumResult controlResult, ChecksumResult testResult)
    {
        return controlResult.getChecksum(getNullCountColumnAlias(column)).equals(controlResult.getChecksum(getAsDoubleNullCountColumnAlias(column))) &&
                testResult.getChecksum(getNullCountColumnAlias(column)).equals(testResult.getChecksum(getAsDoubleNullCountColumnAlias(column)));
    }

    public static String getNullCountColumnAlias(Column column)
    {
        return column.getName() + "$null_count";
    }

    public static String getAsDoubleNullCountColumnAlias(Column column)
    {
        return column.getName() + "_as_double$null_count";
    }
}
