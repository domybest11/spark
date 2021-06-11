package org.apache.spark.util.wechat;

import com.fasterxml.jackson.databind.*;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import retrofit2.Converter;
import retrofit2.Retrofit;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

public class ConverterFactory extends Converter.Factory {
  public static ConverterFactory create() {
    return new ConverterFactory();
  }

  private final ObjectMapper mapper;

  private ConverterFactory() {
    mapper = new ObjectMapper();
  }

  @Override
  public Converter<ResponseBody, ?> responseBodyConverter(Type type, Annotation[] annotations,
                                                          Retrofit retrofit) {
    return new ResponseBodyConverter<>(mapper, type);
  }

  @Override
  public Converter<?, RequestBody> requestBodyConverter(Type type,
                                                        Annotation[] parameterAnnotations,
                                                        Annotation[] methodAnnotations,
                                                        Retrofit retrofit) {
    JavaType javaType = mapper.getTypeFactory().constructType(type);
    ObjectWriter writer = mapper.writerFor(javaType);
    return new RequestBodyConverter<>(writer);
  }

  static final class ResponseBodyConverter<T> implements Converter<ResponseBody, T> {
    private final ObjectReader reader;
    private final String       field;

    ResponseBodyConverter(ObjectMapper mapper, Type type) {
      JavaType javaType = mapper.getTypeFactory().constructType(type);
      this.reader = mapper.readerFor(javaType);
      this.field = getJsonFieldValue(type);
    }

    @Override
    public T convert(ResponseBody value) throws IOException {
      try {
        String s = value.string();
        if (field != null) {
          JsonNode tree = reader.readTree(s);
          JsonNode data = tree.get(field);
          if (data == null) {
            throw new IOException("Field " + field + " not exists");
          }
          return reader.readValue(data);
        }
        return reader.readValue(s);
      } finally {
        value.close();
      }
    }

    private static String getJsonFieldValue(Type type) {
      if (type instanceof Class) {
        Annotation annotation = ((Class) type).getAnnotation(JsonField.class);
        if (annotation != null && annotation instanceof JsonField) {
          return ((JsonField) annotation).value();
        }
      }
      return null;
    }
  }

  static final class RequestBodyConverter<T> implements Converter<T, RequestBody> {
    private static final MediaType MEDIA_TYPE = MediaType
        .parse("application/json; charset=UTF-8");

    private final ObjectWriter adapter;

    RequestBodyConverter(ObjectWriter adapter) {
      this.adapter = adapter;
    }

    @Override
    public RequestBody convert(T value) throws IOException {
      byte[] bytes = adapter.writeValueAsBytes(value);
      return RequestBody.create(MEDIA_TYPE, bytes);
    }
  }
}
