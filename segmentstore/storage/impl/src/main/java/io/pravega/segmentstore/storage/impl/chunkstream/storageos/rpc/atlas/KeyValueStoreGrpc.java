package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 * <pre>
 * Interface exported by the server.
 * </pre>
 */
@javax.annotation.Generated(
        value = "by gRPC proto compiler (version 1.20.0)",
        comments = "Source: internal/pkg/rpc/atlas/kvservice.proto")
public final class KeyValueStoreGrpc {

    public static final String SERVICE_NAME = "atlaspb.KeyValueStore";
    private static final int METHODID_GET_KEY = 0;
    private static final int METHODID_PUT_KEY = 1;
    private static final int METHODID_DELETE_KEY = 2;
    private static final int METHODID_LIST_KEYS = 3;
    private static final int METHODID_BULK_UPDATE_KEYS = 4;
    // Static method descriptors that strictly reflect the proto.
    private static volatile io.grpc.MethodDescriptor<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyRequest,
            io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyResponse> getGetKeyMethod;
    private static volatile io.grpc.MethodDescriptor<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyRequest,
            io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyResponse> getPutKeyMethod;
    private static volatile io.grpc.MethodDescriptor<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyRequest,
            io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyResponse> getDeleteKeyMethod;
    private static volatile io.grpc.MethodDescriptor<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyRequest,
            io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyResponse> getListKeysMethod;
    private static volatile io.grpc.MethodDescriptor<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyRequest,
            io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyResponse> getBulkUpdateKeysMethod;
    private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

    private KeyValueStoreGrpc() {
    }

    @io.grpc.stub.annotations.RpcMethod(
            fullMethodName = SERVICE_NAME + '/' + "GetKey",
            requestType = io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyRequest.class,
            responseType = io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyResponse.class,
            methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyRequest,
            io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyResponse> getGetKeyMethod() {
        io.grpc.MethodDescriptor<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyRequest, io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyResponse> getGetKeyMethod;
        if ((getGetKeyMethod = KeyValueStoreGrpc.getGetKeyMethod) == null) {
            synchronized (KeyValueStoreGrpc.class) {
                if ((getGetKeyMethod = KeyValueStoreGrpc.getGetKeyMethod) == null) {
                    KeyValueStoreGrpc.getGetKeyMethod = getGetKeyMethod =
                            io.grpc.MethodDescriptor.<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyRequest, io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyResponse>newBuilder()
                                                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                                    .setFullMethodName(generateFullMethodName(
                                                            "atlaspb.KeyValueStore", "GetKey"))
                                                    .setSampledToLocalTracing(true)
                                                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                                            io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyRequest.getDefaultInstance()))
                                                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                                            io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyResponse.getDefaultInstance()))
                                                    .setSchemaDescriptor(new KeyValueStoreMethodDescriptorSupplier("GetKey"))
                                                    .build();
                }
            }
        }
        return getGetKeyMethod;
    }

    @io.grpc.stub.annotations.RpcMethod(
            fullMethodName = SERVICE_NAME + '/' + "PutKey",
            requestType = io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyRequest.class,
            responseType = io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyResponse.class,
            methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyRequest,
            io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyResponse> getPutKeyMethod() {
        io.grpc.MethodDescriptor<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyRequest, io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyResponse> getPutKeyMethod;
        if ((getPutKeyMethod = KeyValueStoreGrpc.getPutKeyMethod) == null) {
            synchronized (KeyValueStoreGrpc.class) {
                if ((getPutKeyMethod = KeyValueStoreGrpc.getPutKeyMethod) == null) {
                    KeyValueStoreGrpc.getPutKeyMethod = getPutKeyMethod =
                            io.grpc.MethodDescriptor.<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyRequest, io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyResponse>newBuilder()
                                                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                                    .setFullMethodName(generateFullMethodName(
                                                            "atlaspb.KeyValueStore", "PutKey"))
                                                    .setSampledToLocalTracing(true)
                                                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                                            io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyRequest.getDefaultInstance()))
                                                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                                            io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyResponse.getDefaultInstance()))
                                                    .setSchemaDescriptor(new KeyValueStoreMethodDescriptorSupplier("PutKey"))
                                                    .build();
                }
            }
        }
        return getPutKeyMethod;
    }

    @io.grpc.stub.annotations.RpcMethod(
            fullMethodName = SERVICE_NAME + '/' + "DeleteKey",
            requestType = io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyRequest.class,
            responseType = io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyResponse.class,
            methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyRequest,
            io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyResponse> getDeleteKeyMethod() {
        io.grpc.MethodDescriptor<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyRequest, io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyResponse> getDeleteKeyMethod;
        if ((getDeleteKeyMethod = KeyValueStoreGrpc.getDeleteKeyMethod) == null) {
            synchronized (KeyValueStoreGrpc.class) {
                if ((getDeleteKeyMethod = KeyValueStoreGrpc.getDeleteKeyMethod) == null) {
                    KeyValueStoreGrpc.getDeleteKeyMethod = getDeleteKeyMethod =
                            io.grpc.MethodDescriptor.<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyRequest, io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyResponse>newBuilder()
                                                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                                    .setFullMethodName(generateFullMethodName(
                                                            "atlaspb.KeyValueStore", "DeleteKey"))
                                                    .setSampledToLocalTracing(true)
                                                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                                            io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyRequest.getDefaultInstance()))
                                                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                                            io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyResponse.getDefaultInstance()))
                                                    .setSchemaDescriptor(new KeyValueStoreMethodDescriptorSupplier("DeleteKey"))
                                                    .build();
                }
            }
        }
        return getDeleteKeyMethod;
    }

    @io.grpc.stub.annotations.RpcMethod(
            fullMethodName = SERVICE_NAME + '/' + "ListKeys",
            requestType = io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyRequest.class,
            responseType = io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyResponse.class,
            methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyRequest,
            io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyResponse> getListKeysMethod() {
        io.grpc.MethodDescriptor<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyRequest, io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyResponse> getListKeysMethod;
        if ((getListKeysMethod = KeyValueStoreGrpc.getListKeysMethod) == null) {
            synchronized (KeyValueStoreGrpc.class) {
                if ((getListKeysMethod = KeyValueStoreGrpc.getListKeysMethod) == null) {
                    KeyValueStoreGrpc.getListKeysMethod = getListKeysMethod =
                            io.grpc.MethodDescriptor.<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyRequest, io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyResponse>newBuilder()
                                                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                                    .setFullMethodName(generateFullMethodName(
                                                            "atlaspb.KeyValueStore", "ListKeys"))
                                                    .setSampledToLocalTracing(true)
                                                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                                            io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyRequest.getDefaultInstance()))
                                                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                                            io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyResponse.getDefaultInstance()))
                                                    .setSchemaDescriptor(new KeyValueStoreMethodDescriptorSupplier("ListKeys"))
                                                    .build();
                }
            }
        }
        return getListKeysMethod;
    }

    @io.grpc.stub.annotations.RpcMethod(
            fullMethodName = SERVICE_NAME + '/' + "BulkUpdateKeys",
            requestType = io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyRequest.class,
            responseType = io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyResponse.class,
            methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyRequest,
            io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyResponse> getBulkUpdateKeysMethod() {
        io.grpc.MethodDescriptor<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyRequest, io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyResponse> getBulkUpdateKeysMethod;
        if ((getBulkUpdateKeysMethod = KeyValueStoreGrpc.getBulkUpdateKeysMethod) == null) {
            synchronized (KeyValueStoreGrpc.class) {
                if ((getBulkUpdateKeysMethod = KeyValueStoreGrpc.getBulkUpdateKeysMethod) == null) {
                    KeyValueStoreGrpc.getBulkUpdateKeysMethod = getBulkUpdateKeysMethod =
                            io.grpc.MethodDescriptor.<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyRequest, io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyResponse>newBuilder()
                                                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                                    .setFullMethodName(generateFullMethodName(
                                                            "atlaspb.KeyValueStore", "BulkUpdateKeys"))
                                                    .setSampledToLocalTracing(true)
                                                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                                            io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyRequest.getDefaultInstance()))
                                                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                                            io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyResponse.getDefaultInstance()))
                                                    .setSchemaDescriptor(new KeyValueStoreMethodDescriptorSupplier("BulkUpdateKeys"))
                                                    .build();
                }
            }
        }
        return getBulkUpdateKeysMethod;
    }

    /**
     * Creates a new async stub that supports all call types for the service
     */
    public static KeyValueStoreStub newStub(io.grpc.Channel channel) {
        return new KeyValueStoreStub(channel);
    }

    /**
     * Creates a new blocking-style stub that supports unary and streaming output calls on the service
     */
    public static KeyValueStoreBlockingStub newBlockingStub(
            io.grpc.Channel channel) {
        return new KeyValueStoreBlockingStub(channel);
    }

    /**
     * Creates a new ListenableFuture-style stub that supports unary calls on the service
     */
    public static KeyValueStoreFutureStub newFutureStub(
            io.grpc.Channel channel) {
        return new KeyValueStoreFutureStub(channel);
    }

    public static io.grpc.ServiceDescriptor getServiceDescriptor() {
        io.grpc.ServiceDescriptor result = serviceDescriptor;
        if (result == null) {
            synchronized (KeyValueStoreGrpc.class) {
                result = serviceDescriptor;
                if (result == null) {
                    serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
                                                                          .setSchemaDescriptor(new KeyValueStoreFileDescriptorSupplier())
                                                                          .addMethod(getGetKeyMethod())
                                                                          .addMethod(getPutKeyMethod())
                                                                          .addMethod(getDeleteKeyMethod())
                                                                          .addMethod(getListKeysMethod())
                                                                          .addMethod(getBulkUpdateKeysMethod())
                                                                          .build();
                }
            }
        }
        return result;
    }

    /**
     * <pre>
     * Interface exported by the server.
     * </pre>
     */
    public static abstract class KeyValueStoreImplBase implements io.grpc.BindableService {

        /**
         *
         */
        public void getKey(io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyRequest request,
                           io.grpc.stub.StreamObserver<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyResponse> responseObserver) {
            asyncUnimplementedUnaryCall(getGetKeyMethod(), responseObserver);
        }

        /**
         *
         */
        public void putKey(io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyRequest request,
                           io.grpc.stub.StreamObserver<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyResponse> responseObserver) {
            asyncUnimplementedUnaryCall(getPutKeyMethod(), responseObserver);
        }

        /**
         *
         */
        public void deleteKey(io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyRequest request,
                              io.grpc.stub.StreamObserver<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyResponse> responseObserver) {
            asyncUnimplementedUnaryCall(getDeleteKeyMethod(), responseObserver);
        }

        /**
         *
         */
        public void listKeys(io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyRequest request,
                             io.grpc.stub.StreamObserver<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyResponse> responseObserver) {
            asyncUnimplementedUnaryCall(getListKeysMethod(), responseObserver);
        }

        /**
         *
         */
        public void bulkUpdateKeys(io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyRequest request,
                                   io.grpc.stub.StreamObserver<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyResponse> responseObserver) {
            asyncUnimplementedUnaryCall(getBulkUpdateKeysMethod(), responseObserver);
        }

        @Override
        public final io.grpc.ServerServiceDefinition bindService() {
            return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
                                                  .addMethod(
                                                          getGetKeyMethod(),
                                                          asyncUnaryCall(
                                                                  new MethodHandlers<
                                                                          io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyRequest,
                                                                          io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyResponse>(
                                                                          this, METHODID_GET_KEY)))
                                                  .addMethod(
                                                          getPutKeyMethod(),
                                                          asyncUnaryCall(
                                                                  new MethodHandlers<
                                                                          io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyRequest,
                                                                          io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyResponse>(
                                                                          this, METHODID_PUT_KEY)))
                                                  .addMethod(
                                                          getDeleteKeyMethod(),
                                                          asyncUnaryCall(
                                                                  new MethodHandlers<
                                                                          io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyRequest,
                                                                          io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyResponse>(
                                                                          this, METHODID_DELETE_KEY)))
                                                  .addMethod(
                                                          getListKeysMethod(),
                                                          asyncUnaryCall(
                                                                  new MethodHandlers<
                                                                          io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyRequest,
                                                                          io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyResponse>(
                                                                          this, METHODID_LIST_KEYS)))
                                                  .addMethod(
                                                          getBulkUpdateKeysMethod(),
                                                          asyncUnaryCall(
                                                                  new MethodHandlers<
                                                                          io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyRequest,
                                                                          io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyResponse>(
                                                                          this, METHODID_BULK_UPDATE_KEYS)))
                                                  .build();
        }
    }

    /**
     * <pre>
     * Interface exported by the server.
     * </pre>
     */
    public static final class KeyValueStoreStub extends io.grpc.stub.AbstractStub<KeyValueStoreStub> {
        private KeyValueStoreStub(io.grpc.Channel channel) {
            super(channel);
        }

        private KeyValueStoreStub(io.grpc.Channel channel,
                                  io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @Override
        protected KeyValueStoreStub build(io.grpc.Channel channel,
                                          io.grpc.CallOptions callOptions) {
            return new KeyValueStoreStub(channel, callOptions);
        }

        /**
         *
         */
        public void getKey(io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyRequest request,
                           io.grpc.stub.StreamObserver<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyResponse> responseObserver) {
            ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getGetKeyMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void putKey(io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyRequest request,
                           io.grpc.stub.StreamObserver<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyResponse> responseObserver) {
            ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getPutKeyMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void deleteKey(io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyRequest request,
                              io.grpc.stub.StreamObserver<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyResponse> responseObserver) {
            ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getDeleteKeyMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void listKeys(io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyRequest request,
                             io.grpc.stub.StreamObserver<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyResponse> responseObserver) {
            ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getListKeysMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void bulkUpdateKeys(io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyRequest request,
                                   io.grpc.stub.StreamObserver<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyResponse> responseObserver) {
            ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getBulkUpdateKeysMethod(), getCallOptions()), request, responseObserver);
        }
    }

    /**
     * <pre>
     * Interface exported by the server.
     * </pre>
     */
    public static final class KeyValueStoreBlockingStub extends io.grpc.stub.AbstractStub<KeyValueStoreBlockingStub> {
        private KeyValueStoreBlockingStub(io.grpc.Channel channel) {
            super(channel);
        }

        private KeyValueStoreBlockingStub(io.grpc.Channel channel,
                                          io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @Override
        protected KeyValueStoreBlockingStub build(io.grpc.Channel channel,
                                                  io.grpc.CallOptions callOptions) {
            return new KeyValueStoreBlockingStub(channel, callOptions);
        }

        /**
         *
         */
        public io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyResponse getKey(io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyRequest request) {
            return blockingUnaryCall(
                    getChannel(), getGetKeyMethod(), getCallOptions(), request);
        }

        /**
         *
         */
        public io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyResponse putKey(io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyRequest request) {
            return blockingUnaryCall(
                    getChannel(), getPutKeyMethod(), getCallOptions(), request);
        }

        /**
         *
         */
        public io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyResponse deleteKey(io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyRequest request) {
            return blockingUnaryCall(
                    getChannel(), getDeleteKeyMethod(), getCallOptions(), request);
        }

        /**
         *
         */
        public io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyResponse listKeys(io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyRequest request) {

            return blockingUnaryCall(
                    getChannel(), getListKeysMethod(), getCallOptions(), request);
        }

        /**
         *
         */
        public io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyResponse bulkUpdateKeys(io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyRequest request) {
            return blockingUnaryCall(
                    getChannel(), getBulkUpdateKeysMethod(), getCallOptions(), request);
        }
    }

    /**
     * <pre>
     * Interface exported by the server.
     * </pre>
     */
    public static final class KeyValueStoreFutureStub extends io.grpc.stub.AbstractStub<KeyValueStoreFutureStub> {
        private KeyValueStoreFutureStub(io.grpc.Channel channel) {
            super(channel);
        }

        private KeyValueStoreFutureStub(io.grpc.Channel channel,
                                        io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @Override
        protected KeyValueStoreFutureStub build(io.grpc.Channel channel,
                                                io.grpc.CallOptions callOptions) {
            return new KeyValueStoreFutureStub(channel, callOptions);
        }

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyResponse> getKey(
                io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyRequest request) {
            return futureUnaryCall(
                    getChannel().newCall(getGetKeyMethod(), getCallOptions()), request);
        }

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyResponse> putKey(
                io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyRequest request) {
            return futureUnaryCall(
                    getChannel().newCall(getPutKeyMethod(), getCallOptions()), request);
        }

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyResponse> deleteKey(
                io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyRequest request) {
            return futureUnaryCall(
                    getChannel().newCall(getDeleteKeyMethod(), getCallOptions()), request);
        }

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyResponse> listKeys(
                io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyRequest request) {
            return futureUnaryCall(
                    getChannel().newCall(getListKeysMethod(), getCallOptions()), request);
        }

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyResponse> bulkUpdateKeys(
                io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyRequest request) {
            return futureUnaryCall(
                    getChannel().newCall(getBulkUpdateKeysMethod(), getCallOptions()), request);
        }
    }

    private static final class MethodHandlers<Req, Resp> implements
                                                         io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
                                                         io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
                                                         io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
                                                         io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
        private final KeyValueStoreImplBase serviceImpl;
        private final int methodId;

        MethodHandlers(KeyValueStoreImplBase serviceImpl, int methodId) {
            this.serviceImpl = serviceImpl;
            this.methodId = methodId;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
            switch (methodId) {
                case METHODID_GET_KEY:
                    serviceImpl.getKey((io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyRequest) request,
                                       (io.grpc.stub.StreamObserver<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.GetKeyResponse>) responseObserver);
                    break;
                case METHODID_PUT_KEY:
                    serviceImpl.putKey((io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyRequest) request,
                                       (io.grpc.stub.StreamObserver<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.PutKeyResponse>) responseObserver);
                    break;
                case METHODID_DELETE_KEY:
                    serviceImpl.deleteKey((io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyRequest) request,
                                          (io.grpc.stub.StreamObserver<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.DeleteKeyResponse>) responseObserver);
                    break;
                case METHODID_LIST_KEYS:
                    serviceImpl.listKeys((io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyRequest) request,
                                         (io.grpc.stub.StreamObserver<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.ListKeyResponse>) responseObserver);
                    break;
                case METHODID_BULK_UPDATE_KEYS:
                    serviceImpl.bulkUpdateKeys((io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyRequest) request,
                                               (io.grpc.stub.StreamObserver<io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.BulkUpdateKeyResponse>) responseObserver);
                    break;
                default:
                    throw new AssertionError();
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public io.grpc.stub.StreamObserver<Req> invoke(
                io.grpc.stub.StreamObserver<Resp> responseObserver) {
            switch (methodId) {
                default:
                    throw new AssertionError();
            }
        }
    }

    private static abstract class KeyValueStoreBaseDescriptorSupplier
            implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
        KeyValueStoreBaseDescriptorSupplier() {
        }

        @Override
        public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
            return io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.getDescriptor();
        }

        @Override
        public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
            return getFileDescriptor().findServiceByName("KeyValueStore");
        }
    }

    private static final class KeyValueStoreFileDescriptorSupplier
            extends KeyValueStoreBaseDescriptorSupplier {
        KeyValueStoreFileDescriptorSupplier() {
        }
    }

    private static final class KeyValueStoreMethodDescriptorSupplier
            extends KeyValueStoreBaseDescriptorSupplier
            implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
        private final String methodName;

        KeyValueStoreMethodDescriptorSupplier(String methodName) {
            this.methodName = methodName;
        }

        @Override
        public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
            return getServiceDescriptor().findMethodByName(methodName);
        }
    }
}
