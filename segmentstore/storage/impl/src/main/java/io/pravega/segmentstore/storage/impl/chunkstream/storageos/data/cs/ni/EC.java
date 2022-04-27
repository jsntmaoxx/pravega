package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni;

public class EC {
    public static native boolean init();

    public static native long make_encode_gf_table(int data_num, int code_num);

    public static native long make_decode_gf_table(int data_num, int code_num, boolean[] v_state, int error_num);

    public static native long make_decode_gf_table(int data_num, int code_num, boolean[] v_state, int error_num, int decode_index);

    public static native long make_decode_gf_table(int data_num, int code_num, boolean[] v_state, int error_num, int decode_index1, int decode_index2);

    public static native long make_decode_gf_table(int data_num, int code_num, boolean[] v_state, int error_num, int[] v_decode_index);

    public static native void destroy_gf_table(long gf_table);

    public static native long make_buffer(int length, int item_size);

    public static native void destroy_buffer(long buffer);

    public static native boolean encode(int data_num, int code_num, int seg_len, long gf_table, long data_ary_ptr, long code_ary_ptr);

    public static native boolean encode_update_seg(int data_num, int code_num, int seg_index, int seg_len, long gf_table, long seg_ptr, long code_ary_ptr);

    public static native boolean encode_update_seg_range(int data_num, int code_num, int seg_index, int seg_offset, int seg_len, long gf_table, long data_ptr, long code_ary_ptr);
}
