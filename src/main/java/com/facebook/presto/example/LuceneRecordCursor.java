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
package com.facebook.presto.example;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
//import com.google.common.base.Throwables;
//import com.google.common.io.ByteSource;
//import com.google.common.io.CountingInputStream;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;
import org.apache.lucene.search.ScoreDoc;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

public class LuceneRecordCursor
        implements RecordCursor
{
    private static final Splitter LINE_SPLITTER = Splitter.on(",").trimResults();

    private final List<LuceneColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;

    private final long totalBytes = 0;
    private List<String> fields;
    private ScoreDoc[] hits;
    private IndexSearcher searcher;
    private int CurrentDocIdx = 0;
    private int NumDoc = 0;
    static private Analyzer a = null;

    
    public LuceneRecordCursor(List<LuceneColumnHandle> columnHandles) throws ParseException{
    	
    	this.columnHandles = columnHandles;
    	
	    IndexReader reader = null;
		try {
			reader = DirectoryReader.open(FSDirectory.open(Paths.get("/home/liyong/workspace-neno/lucenetest/index")));
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    searcher = new IndexSearcher(reader);
	    this.NumDoc = reader.maxDoc();
    	
    	fieldToColumnIndex = new int[columnHandles.size()];
    	for (int i = 0; i < columnHandles.size(); i++)
    	{	
    		LuceneColumnHandle columnHandle = columnHandles.get(i);
    		fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
    	}	
    }

    @Override
    public long getTotalBytes()
    {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
    	
        if (CurrentDocIdx == NumDoc) {
            return false;
        }
        Document doc = null;
		try {
			doc = searcher.doc(CurrentDocIdx++);
	        fields = parseDoc(doc);
		} catch (IOException e) 
		{
			e.printStackTrace();
		}

        return true;
    }
    
    private List<String> parseDoc(Document doc)
    {
    	List<String> fds = new ArrayList<String>(); 	
    	String allFieldNames = "orderkey,custkey,orderstatus,totalprice,orderdate,orderpriority,clerk,shippriority,comment";
    	String fieldNames[] = allFieldNames.split(",");
    	for(int i = 0; i < fieldNames.length; i++){
    		
    		String fieldName = fieldNames[i];
    		String columnValue = doc.get(fieldName);
    		fds.add(columnValue);
    	}
    	
    	return fds;
    }

    private String getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");

        int columnIndex = fieldToColumnIndex[field];
        return fields.get(columnIndex);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT);
        return Long.parseLong(getFieldValue(field));
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice(getFieldValue(field));
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
    }
}
