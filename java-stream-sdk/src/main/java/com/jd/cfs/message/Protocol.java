package com.jd.cfs.message;


public class Protocol {
    public final static int  HEAD_SIZE          = 45;
    public final static byte MAGIC              = (byte) 0xFF;
    public final static byte OK                 = (byte) 0XF0;
    public final static byte EXTENT             = (byte) 1;
    public final static byte PACKET_POWER       = 16;
    public final static int  EXTENT_PACKET_SIZE = 1 << PACKET_POWER;
    public final static    int    MAGIC_INDEX = 0;
    public final static    int    OP_CODE_INDEX = 2;
    public final static    int    RESULT_CODE_INDEX = 3;
    public final static    int    CRC_INDEX = 5;
    public final static    int    SIZE_INDEX = 9;
    public final static    int    FILE_ID_INDEX = 21;
    public final static    int    REQUEST_ID_INDEX = 37;
    public enum Command {
        Create(0x01), Delete(0x02), Write(0x03), Read(0x04), SRead(0x05);
        public byte code;

        Command(int code) {
            this.code = (byte) code;
        }
    }

    public enum ErrorCode {
        FILE_NOT_EXITS(0xF5),OpIntraGroupNetErr(0xF3),OpArgUnmatchErr(0xF4)
        ,OpDiskNoSpaceErr(0xF6),OpDiskErr(0xF7), OpErr(0xF8);
        public byte code;

        ErrorCode(int code) {
            this.code = (byte) code;
        }
    }
}
