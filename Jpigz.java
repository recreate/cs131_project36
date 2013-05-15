import java.lang.Thread;
import java.io.*;
import java.util.zip.*;

public class Jpigz implements Runnable {

    // Taken from java.util.zip.GZIPOutputStream
    private final static int GZIP_MAGIC = 0x8b1f;
    private final static int HEADER_SIZE = 10;
    private final static int TRAILER_SIZE = 8;

    private final static byte[] header = {
        (byte) GZIP_MAGIC,                // Magic number (short)
        (byte)(GZIP_MAGIC >> 8),          // Magic number (short)
        Deflater.DEFLATED,                // Compression method (CM)
        0,                                // Flags (FLG)
        0,                                // Modification time MTIME (int)
        0,                                // Modification time MTIME (int)
        0,                                // Modification time MTIME (int)
        0,                                // Modification time MTIME (int)
        0,                                // Extra flags (XFLG)
        0                                 // Operating system (OS)
    };

    /*
    * Writes GZIP member trailer to a byte array, starting at a given
    * offset.
    */
    private static void writeTrailer(byte[] buf, int offset, int crc, int size)
             throws IOException {
        writeInt(crc, buf, offset); // CRC-32 of uncompr. data
        writeInt(size, buf, offset + 4); //Number of uncompr. bytes
    }

    /*
    * Writes integer in Intel byte order to a byte array, starting at a
    * given offset.
    */
    private static void writeInt(int i, byte[] buf, int offset) 
            throws IOException {
        writeShort(i & 0xffff, buf, offset);
        writeShort((i >> 16) & 0xffff, buf, offset + 2);
    }

    /*
    * Writes short integer in Intel byte order to a byte array, starting
    * at a given offset
    */
    private static void writeShort(int s, byte[] buf, int offset) 
            throws IOException {
        buf[offset] = (byte)(s & 0xff);
        buf[offset + 1] = (byte)((s >> 8) & 0xff);
    }

    public static final int MAXIMUM_BLOCKS = 65536; // Arbitrary limit
    public static final int BLOCK_SIZE = 128 * 1024;
    public static final int DICTIONARY_SIZE = 32;
    
    private byte[] storage;
    private byte[] compression;
    public int compressionLength;
    public int bsize;
    public boolean finished;

    public Jpigz () {
        this.storage = new byte[BLOCK_SIZE];
        this.compression = new byte[HEADER_SIZE + BLOCK_SIZE + TRAILER_SIZE];
        this.compressionLength = 0;
        this.bsize = 0;
        this.finished = false;
    }

    public void run() {

        System.arraycopy(header, 0, this.compression, 0, HEADER_SIZE);

        Deflater def = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
        def.setInput(this.storage, 0, this.bsize);

        /*
            byte[] dictionary = new byte[DICTIONARY_SIZE];
            System.arraycopy(this.storage, 0, dictionary, 0, DICTIONARY_SIZE);
            def.setDictionary(dictionary, 0, DICTIONARY_SIZE);
        */

        def.finish();

        int nbytes = def.deflate(this.compression, HEADER_SIZE, this.bsize, 
            Deflater.SYNC_FLUSH);
        int totalin = def.getTotalIn();
        def.end();

        CRC32 crc = new CRC32();
        crc.reset();
        crc.update(this.storage, 0, this.bsize);
        
        try {
            writeTrailer(this.compression, HEADER_SIZE + nbytes, 
                (int)crc.getValue(), totalin);
        } catch (Exception e) {
            System.err.println(e);
            System.exit(1);
        }

        this.compressionLength = HEADER_SIZE + nbytes + TRAILER_SIZE;
        this.finished = true;
    }

    public static void main (String[] args) {

        boolean independent = false;
        int num_processors = Runtime.getRuntime().availableProcessors();

        // Get command line arguments
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-i")) {
                independent = true;
                continue;
            }
            if (args[i].equals("-p") && i < args.length - 1) {
                int procs = Integer.parseInt(args[i+1]);
                if (procs > num_processors || procs < 0) {
                    System.err.println("Too many processors specified");
                    System.exit(1);
                }
                num_processors = procs;
            }
        }

        byte[] block = new byte[BLOCK_SIZE];
        int bytesRead = 0;
        int tn = 0;
        int i = 0;
        
        // Arbitrary maximum file size: 65536 * (128 * 1024) bytes
        Jpigz[] compressors = new Jpigz[MAXIMUM_BLOCKS];
        Thread[] threads = new Thread[num_processors];

        for (; true; tn++) {

            if (tn == num_processors) {
                tn = -1;
                continue;
            }

            if (threads[tn] != null && threads[tn].isAlive()) {
                // Wait for threads to become available
                continue;
            }

            compressors[i] = new Jpigz();
            
            try {
                bytesRead = System.in.read(compressors[i].storage, 0, BLOCK_SIZE);
                if (bytesRead == -1) {
                    i--;
                    break;
                }
                compressors[i].bsize = bytesRead;
            } catch (Exception e) {
                System.err.println(e);
                System.exit(1);
            }

            threads[tn] = new Thread(compressors[i]);
            (threads[tn]).start();

            if (bytesRead != BLOCK_SIZE) {
                break;
            }

            i++;
        }

        for (int j = 0; j <= i; j++) {
            // Wait for threads to finish deflating (or to start running)
            if (!compressors[j].finished) {
                j--;
                continue;
            }
            System.out.write(compressors[j].compression, 0, 
                compressors[j].compressionLength);
        }

    }

}

