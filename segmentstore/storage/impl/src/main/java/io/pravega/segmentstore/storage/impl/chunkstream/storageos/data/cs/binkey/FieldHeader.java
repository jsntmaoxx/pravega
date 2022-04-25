package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.binkey;

/**
 * For efficiency, field headers are represented in memory with a Java `long`. This lets us parametrize how
 * field headers can be constructed with a builder-like pattern while avoiding the need for any memory allocations.
 * <p>
 * A single field number can be passed directly to KeyBuilder for convenience, or a pre-constructed field header,
 * build using this class, can be passed in. KeyBuilder uses the BUILT_FIELD_HEADER_FLAG to determine how to
 * interpret the value given to it.
 * <p>
 * The encoded field header in the 'long' value returned should be considered opaque by the caller and passed
 * directly to KeyBuilder. The exact format is a packed version of the format described in the design document
 * https://TODO.todo
 */
public class FieldHeader {
    /**
     * This bit being set in a 'long' indicates that it contains a pre-built field header and should be
     * written out as-is.
     */
    static final long BUILT_FIELD_HEADER_FLAG = 1L << 62;

    /**
     * Builds a field header consisting of a single field number.
     *
     * @param fieldNumber The number for the field being encoded (usually the GPB field number).
     * @return An encoded field header suitable for passing directly to KeyBuilder.
     */
    public static long build(int fieldNumber) {
        return BUILT_FIELD_HEADER_FLAG | encodeFieldNumber(fieldNumber);
    }

    /**
     * Builds an extended field header for inserting a new field _just before_ the next field, even if there
     * are no gaps in the field number integers.
     *
     * @param nextFieldNumber1 The number of the next field in the key (usually the GPB field number of the
     *                         next field in the key).
     * @param fieldNumber      The number for the field being encoded (usually the GPB field number).
     * @return An encoded field header suitable for passing directly to KeyBuilder.
     */
    public static long build(int nextFieldNumber1, int fieldNumber) {
        return BUILT_FIELD_HEADER_FLAG |
               encodeExtFieldNumber(nextFieldNumber1) |
               (encodeFieldNumber(fieldNumber) << 8);
    }

    /**
     * Builds an extended field header for inserting a new field _just before_ the next field, when the
     * "next" field header has already been encoded using #build(int, int).
     *
     * @param nextFieldNumber1 The first field number used in the next field's field header.
     * @param nextFieldNumber2 The second field number used in the next field's field header.
     * @param fieldNumber      The number for the field being encoded (usually the GPB field number).
     * @return An encoded field header suitable for passing directly to KeyBuilder.
     */
    public static long build(int nextFieldNumber1, int nextFieldNumber2, int fieldNumber) {
        return BUILT_FIELD_HEADER_FLAG |
               encodeExtFieldNumber(nextFieldNumber1) |
               (encodeExtFieldNumber(nextFieldNumber2) << 8) |
               (encodeFieldNumber(fieldNumber) << 16);
    }

    // ^ This pattern can be continued to infinity, but hopefully no key ever evolves so much that its field
    // ordering becomes so mangled...


    // Encodes a single field number
    //
    // The negation causes the sort order of field numbers to become inverted when comparing keys. The
    // left shift reserves the least significant bit as like a 0.5 fractional value to allow inserting
    // new values between integers. When the 0.5 bit is set, it implies there's (at least) one more byte
    // in the field header.
    static long encodeFieldNumber(int fieldNumber) {
        if ((fieldNumber < 1) || (fieldNumber > 127))
            throw new IllegalArgumentException(String.format("Field number out of range 1-127 (%d)", fieldNumber));

        return (-fieldNumber << 1) & 0xff;
    }

    // Encode one of the "extended" field numbers - the extra numbers inserted before the real field number
    // to help coax sorting order.
    static long encodeExtFieldNumber(int fieldNumber) {
        return encodeFieldNumber(fieldNumber) | 0x01L;
    }
}
