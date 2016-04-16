package communication;

import java.io.*;

/**
 * Created by frm on 07/03/16.
 */
public class Packet implements Serializable {
    private String id;
    private Object content;

    public Packet(String id, Object content) {
        this.id = id;
        this.content = content;
    }


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

    public String getId() {
        return id;
    }

    public Object getContent() {
        return content;
    }

}
