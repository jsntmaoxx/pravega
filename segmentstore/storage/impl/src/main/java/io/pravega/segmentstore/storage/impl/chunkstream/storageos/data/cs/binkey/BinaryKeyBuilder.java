package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.binkey;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Utf8;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import com.google.protobuf.ProtocolMessageEnum;
import com.google.protobuf.UnsafeByteOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;


/**
 * Helper class for building a binary key from fields in a GPB object.
 * <p>
 * The key is constructed in a way that it will remain sortable using standard unsigned byte array comparisons
 * (such as the C {@code memcmp} function).
 */
public class BinaryKeyBuilder {

    private static final Logger logger = LoggerFactory.getLogger(BinaryKeyBuilder.class);
    private static final int FIELD_HEADER_SIZE = 1;
    private static final int INT_SIZE = 4;
    private static final int LONG_SIZE = 8;
    private static final int DOUBLE_SIZE = 8;
    private static final int SHA256_SIZE = 32;
    /**
     * Enables the 'corrupted string workaround' so that putString() will allow invalid ASCII characters to be
     * passed in but they are clamped into the range [1, 127].  This allows strings with random bit flips to be
     * encoded at the cost of no input validation (STORAGE-29181).
     */
    private static boolean enableCorruptedStringWorkaround = false;

    static {
        // Initialize value based on whether this marker file exists or not
        File f = new File("/data/corrupt_key_workaround");
        if (f.exists()) {
            setEnableCorruptedStringWorkaround(true);
            logger.info("Corrupted String Workaround enabled");
        }
    }

    protected ByteBuffer byteBuf;
    protected int subKeyLevel = 0;
    /**
     * The ByteBuffer position of the end of the key prefix.
     * <p>
     * The prefix length might be less than the total length of the key if the last element put might be a
     * variable-length prefix itself (such as a string or a subkey). In that case, the variable-length field terminator
     * is excluded from the prefix.
     * <p>
     * For example, after a 4-character string is put the buffer will contain:
     * <pre>
     *     12 't' 'e' 's' 't' 0
     *     ^                  ^  ^
     *     |                  |   \- byteBuf.position()
     * (fieldId)              |
     *                        \- prefixTailPos
     * </pre>
     * <p>
     * If a prefix is constructed, the trailing 0 will be excluded so that all strings beginning with "test"
     * will match.
     */
    protected int prefixTailPos = 0;
    private int numReallocations = 0;
    private boolean hasCorruptedString = false;

    public BinaryKeyBuilder() {
        this(256);
    }

    public BinaryKeyBuilder(int initialCapacity) {
        byteBuf = ByteBuffer.allocate(initialCapacity);
    }

    public static void setEnableCorruptedStringWorkaround(boolean value) {
        enableCorruptedStringWorkaround = value;
    }

    public static int getStringSize(String string) {
        return Strings.isNullOrEmpty(string) ? 0 : string.length() + 1; // null terminator takes an extra byte
    }

    public static int getEncodedStringSize(ByteString encodedString) {
        if (encodedString == null || encodedString.size() == 0) {
            return 0;
        } else {
            return encodedString.size() + 1; // null terminator takes an extra byte
        }
    }

    public static int getUtf8StringSize(String string) {
        if (Strings.isNullOrEmpty(string)) {
            return 0;
        }
        return Utf8.encodedLength(string) + 1; // null terminator takes an extra byte
    }

    public static int getIntSize() {
        return INT_SIZE;
    }

    public static int getLongSize() {
        return LONG_SIZE;
    }

    public static int getDoubleSize() {
        return DOUBLE_SIZE;
    }

    public static int getFieldHeaderSize(int fieldHeaderNumbers) {
        return fieldHeaderNumbers * FIELD_HEADER_SIZE;
    }

    public static int getSHA256Size(String hashId) {
        if (Strings.isNullOrEmpty(hashId)) {
            return 0;
        }
        byte[] bucketHashIdBytes = BaseEncoding.base16().lowerCase().decode(hashId);
        Preconditions.checkArgument(bucketHashIdBytes.length == SHA256_SIZE, "invalid hashId length");
        return SHA256_SIZE;
    }

    /**
     * Encodes a double in a binary format such that order is preserved. For example if a < b,
     * then encodeDouble(a) < encodeDouble(b). To do this, two operations are performed on the double's bits.
     * <p>
     * First the sign bit (the first bit) is flipped so that a negative number has a 0 in the sign bit,
     * and a positive number has a 1 in the sign bit. This ensures a negative number's bit string will be less than
     * a positive number's bit string.
     * <p>
     * The second operation is only done on negative numbers. If the number is negative, then all the bits are flipped.
     * If we ignore the sign bit, then a negative representation of a number is the same as it's positive representation.
     * This means that if we have two negative numbers, X and Y, where X < Y, then the bitstring(X) > bitstring(Y),
     * because the absolute value of X is greater than the absolute value of Y. To reverse this, we flip the bits of each
     * so that bitstring(X) < bitstring(Y).
     *
     * @param value is the 64 bit floating point number to be encoded.
     * @return Returns the encoded bit string of value.
     */
    private static long encodeDouble(double value) {
        if (value == -0.0) { //Negative zero is not allowed.
            value = 0.0;
        }
        long encodedBits = Double.doubleToLongBits(value);
        if (value < 0) { //Check if negative
            encodedBits ^= 0x7F_FF_FF_FF_FF_FF_FF_FFL; //Flip all but first bit
        }
        encodedBits ^= 0x80_00_00_00_00_00_00_00L; //Flip the first bit
        return encodedBits;
    }

    /**
     * Returns the number of times that the internal buffer had to be expanded since this BinaryKeyBuilder
     */
    public int getNumReallocations() {
        return numReallocations;
    }

    /**
     * When enableCorruptedStringWorkaround is enabled then this returns a boolean value indicating if any corrupted
     * strings were encoded by this BinaryKeyBuilder.
     */
    public boolean hasCorruptedStrings() {
        return hasCorruptedString;
    }

    public BinaryKeyBuilder putInt(long fieldNumber, int value) {
        putFieldHeader(fieldNumber);
        reserve(INT_SIZE);

        byteBuf.putInt(value + Integer.MIN_VALUE);
        // Integers can't be partially matched so include the entire integer in the prefix
        prefixTailPos = byteBuf.position();

        return this;
    }

    public BinaryKeyBuilder putLong(long fieldNumber, long value) {
        putFieldHeader(fieldNumber);
        reserve(LONG_SIZE);
        byteBuf.putLong(value + Long.MIN_VALUE);

        // Integers can't be partially matched so include the entire integer in the prefix
        prefixTailPos = byteBuf.position();

        return this;
    }

    public BinaryKeyBuilder putUnsignedLong(long fieldNumber, long value) {
        putFieldHeader(fieldNumber);
        reserve(LONG_SIZE);
        byteBuf.putLong(value);  ///<- the adjustment is not needed because the value is not really signed

        // Integers can't be partially matched so include the entire integer in the prefix
        prefixTailPos = byteBuf.position();

        return this;
    }

    public BinaryKeyBuilder putDouble(long fieldNumber, double value) {
        putFieldHeader(fieldNumber);
        reserve(DOUBLE_SIZE);

        byteBuf.putLong(encodeDouble(value));
        prefixTailPos = byteBuf.position();
        return this;
    }

    public BinaryKeyBuilder putFixedLengthByteArray(long fieldNumber, byte[] data) {
        putFieldHeader(fieldNumber);
        reserve(data.length);
        byteBuf.put(data);
        // byte array should not be partially matched, so include entire length in prefix.
        prefixTailPos = byteBuf.position();
        return this;
    }

    /**
     * Put an ASCII string.
     */
    public BinaryKeyBuilder putString(long fieldNumber, String value) {
        putFieldHeader(fieldNumber);

        reserve(value.length() + 1);
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if ((ch == 0) || (ch > 0x7f)) {
                if (!enableCorruptedStringWorkaround) {
                    throw new IllegalArgumentException(String.format("Illegal ASCII character (%d) at position %d", (int) ch, i));
                }
                // Fix up this corrupted character
                ch = (ch == 0) ? (char) 0x01 : (char) 0x7f;
                hasCorruptedString = true;
            }
            byteBuf.put((byte) ch);
        }

        // Strings can be partially matched. Only include the characters specified in the prefix and not the 0 terminator.
        prefixTailPos = byteBuf.position();

        byteBuf.put((byte) 0);  // NUL terminate the string
        return this;
    }

    /**
     * Put a Unicode string using UTF-8 encoding.
     */
    public BinaryKeyBuilder putUtf8String(long fieldNumber, String value) {
        try {
            final byte[] utf8Value = value.getBytes("utf-8");

            putFieldHeader(fieldNumber);
            reserve(utf8Value.length + 1);
            byteBuf.put(utf8Value);

            // Strings can be partially matched. Only include the characters specified in the prefix and not the 0 terminator.
            prefixTailPos = byteBuf.position();

            byteBuf.put((byte) 0);  // NUL terminate the string
        } catch (UnsupportedEncodingException e) {
            // This should never happen
            throw new RuntimeException("JRE is busted", e);
        }
        return this;
    }

    /**
     * Put a string that has already been encoded into a byte array using the ASCII character set.
     * <p>
     * Note: ASCII is a subset of UTF-8 encoding as long as all the characters are in the range 1-127.
     */
    public BinaryKeyBuilder putEncodedString(long fieldNumber, ByteString value) {
        putFieldHeader(fieldNumber);

        reserve(value.size() + 1);
        for (int i = 0; i < value.size(); i++) {
            byte b = value.byteAt(i);
            if (b <= 0) {
                throw new IllegalArgumentException(String.format("Illegal ASCII character at position %d", i));
            }
            byteBuf.put(b);
        }

        // Strings can be partially matched. Only include the characters specified in the prefix and not the 0 terminator.
        prefixTailPos = byteBuf.position();

        byteBuf.put((byte) 0);  // NUL terminate the string
        return this;
    }

    // When building a prefix, match an exact full string.
    //
    // This is like putString, however it bumps up the prefix length to include the string
    // NUL termination byte so that the entire string must match.
    //
    // When building a full key, this is no different than putString().
    //
    // FIXME: This is poorly named. Perhaps mixing key building and prefix building in the same builder
    // is not a good idea.
    public BinaryKeyBuilder putStringExactMatch(long fieldNumber, String value) {
        putString(fieldNumber, value);
        prefixTailPos = byteBuf.position();
        return this;
    }

    public BinaryKeyBuilder putEnum(long fieldNumber, ProtocolMessageEnum value) {
        return putInt(fieldNumber, value.getNumber());
    }

    public BinaryKeyBuilder putBoolean(long fieldNumber, boolean value) {
        putFieldHeader(fieldNumber);
        reserve(1);
        byteBuf.put(value ? (byte) 1 : (byte) 0);

        // Booleans can't be partially matched so include the entire thing in the prefix
        prefixTailPos = byteBuf.position();

        return this;
    }

    /**
     * Insert a pre-built key as a subkey.
     *
     * @param fieldNumber The GPB field number of the subkey field.
     * @param keyData     The pre-built key being inserted. Since the subkey is inserted directly into the key, it should
     *                    have been built with another {@code BinaryKeyBuilder}.
     * @return This BinaryKeyBuilder
     */
    public BinaryKeyBuilder putSubKey(long fieldNumber, ByteString keyData) {
        putBeginSubKey(fieldNumber);

        reserve(keyData.size());
        byteBuf.put(keyData.asReadOnlyByteBuffer());

        // Only include the sub-key data in the prefix, not its variable-length field terminator.
        prefixTailPos = byteBuf.position();

        putEndSubKey();
        return this;
    }

    /**
     * Begin inserting a subkey that may contain any number of fields.
     * The end of the subkey must be marked by calling {@code putEndSubKey()}.
     *
     * @param fieldNumber The GPB field number of the subkey field.
     * @return This BinaryKeyBuilder
     */
    public BinaryKeyBuilder putBeginSubKey(long fieldNumber) {
        putFieldHeader(fieldNumber);
        subKeyLevel++;
        // Note: At this point all that has been put is an empty subkey so do not include this in the key prefix
        return this;
    }

    /**
     * Insert an end-of-key marker. Every key must have at least one end-of-key marker, plus one marker for
     * every subkey started with {@code putBeginSubKey()}.
     *
     * @return This BinaryKeyBuilder
     */
    public BinaryKeyBuilder putEndSubKey() {
        if (subKeyLevel == 0) {
            // This is a programming error. Fix your key building code.
            throw new IllegalStateException("Can't terminate a subkey - no subkey being serialized");
        }

        // Field number 0 marks the end of a sub-key
        reserve(1);
        byteBuf.put((byte) 0);
        subKeyLevel--;

        // Note: Do not include this in the prefix (yet). The sub-key may have a trailing string that we want to
        // keep unterminated for now.

        return this;
    }

    public ByteString build() {
        if (subKeyLevel != 0) {
            // This is a programming error. Fix your key building code.
            throw new IllegalStateException(String.format("Incomplete key - %d subkey(s) not terminated", subKeyLevel));
        }

        if (hasCorruptedString) {
            logger.warn("SchemaKey contains a corrupted string {}", byteBufToHex());
        }

        // the former gives us LiteralByteString which is much quicker to iterator over (vs BoundedByteString)
        return !byteBuf.hasRemaining()
               ? UnsafeByteOperations.unsafeWrap(byteBuf.array())
               : UnsafeByteOperations.unsafeWrap(byteBuf.array(), 0, byteBuf.position());
    }

    public ByteString buildPrefix() {
        if (hasCorruptedString) {
            if (BinaryKeyBuilderProperties.isExceptionOnPrefixCorruption()) {
                throw new IllegalArgumentException(String.format("Illegal ASCII character in prefix %s", byteBufToHex()));
            } else {
                logger.warn("SchemaKey prefix contains a corrupted string {}", byteBufToHex());
            }
        }

        return UnsafeByteOperations.unsafeWrap(byteBuf.array(), 0, prefixTailPos);
    }

    /**
     * Returns the key hex representation for printing.
     */
    protected String byteBufToHex() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < byteBuf.position(); i++) {
            sb.append(String.format("%02X", byteBuf.get(i)));
        }
        return sb.toString();
    }

    protected void putFieldHeader(long fieldNumber) {
        if (fieldNumber < 1)
            throw new IllegalArgumentException(String.format("Illegal field number (%d)", fieldNumber));

        if ((fieldNumber & FieldHeader.BUILT_FIELD_HEADER_FLAG) == 0) {
            // This is just a single field number. This is the common case so we perform the encoding
            // of the field header here to save the caller a lot of typing.
            if (fieldNumber > 127)
                throw new IllegalArgumentException(String.format("Illegal field number (%d)", fieldNumber));

            reserve(1);
            byteBuf.put((byte) FieldHeader.encodeFieldNumber((int) fieldNumber));
        } else {
            // This is a pre-formatted field header stored in the long in little-endian format. This is not a common
            // case and requires the caller to use the FieldHeader class to construct this value.
            while ((fieldNumber & 0x01) != 0) {
                // Write out the variable length portion of the header, indicated by the LSB set to 1
                reserve(1);
                byteBuf.put((byte) fieldNumber);
                fieldNumber >>= 8;
            }
            // Write out the final byte of the header, indicated by the LSB set to 0
            reserve(1);
            byteBuf.put((byte) fieldNumber);
        }
    }

    protected void reserve(int size) {
        if (byteBuf.remaining() < size) {
            // Allocate a larger ByteBuffer
            int oldCap = byteBuf.capacity();
            ByteBuffer newByteBuf = ByteBuffer.allocate(oldCap + Math.max(size, Math.min(16, oldCap / 2)));

            byteBuf.flip();
            newByteBuf.put(byteBuf);
            byteBuf = newByteBuf;
            numReallocations++;
        }
    }
}
