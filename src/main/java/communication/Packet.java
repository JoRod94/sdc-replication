package communication;

import java.io.*;

/**
 * Created by frm on 07/03/16.
 * Wrapper for objects to be sent inside a Message.
 * This class allows for auto marshalling and unmarshalling of any content.
 */
public class Packet implements Serializable {
    private String id;
    private Object content;

    /**
     * Creates a packet with a given unique id and the attached object
     * @param id - uniqued packet id
     * @param content - object to be attached
     */
    public Packet(String id, Object content) {
        this.id = id;
        this.content = content;
    }

    /**
     * Creates a packet from the given bytes.
     * Reconstructs and unmarshalls the underlying packet object.
     * @param payload - packet bytes to be unmarshalled
     */
    public Packet(byte[] payload) {
        ByteArrayInputStream bis = new ByteArrayInputStream(payload);
        ObjectInputStream ois = null;

        try {
            ois = new ObjectInputStream(bis);
            Packet m = (Packet) ois.readObject();
            this.id = m.id;
            this.content = m.content;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                bis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                if(ois != null)
                    ois.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Marshalls the current object to a byte representation
     * @return - byte representation of the object
     */
    public byte[] getBytes() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = null;
        byte[] obj = null;
        try {
            oos = new ObjectOutputStream(bos);
            oos.writeObject(this);
            obj = bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(oos != null)
                    oos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                bos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return obj;
    }

    /**
     * @return object unique id
     */
    public String getId() {
        return id;
    }

    /**
     * @return object attached content
     */
    public Object getContent() {
        return content;
    }

}
