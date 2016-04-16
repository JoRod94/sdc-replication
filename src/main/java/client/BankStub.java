package client;

import net.sf.jgcs.*;
import net.sf.jgcs.jgroups.JGroupsGroup;
import net.sf.jgcs.jgroups.JGroupsProtocolFactory;
import net.sf.jgcs.jgroups.JGroupsService;
import communication.Packet;
import communication.Invocation;

import java.io.IOException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by joaorodrigues on 12 Apr 16.
 */
public class BankStub implements MessageListener{
    private String clientId;
    private int msgId;
    private DataSession data;
    private Service service;
    private final Lock replyLock = new ReentrantLock();
    private final Condition replyCondition = replyLock.newCondition();

    // Holds the reply to be shared between the operation and onMessage
    private Object reply;



    public BankStub() throws IOException {
        clientId = (new java.rmi.dgc.VMID()).toString();
        msgId = 0;
        setUpConnection();
    }

    // Acquires JGroups variables and joins the group
    public void setUpConnection() throws GroupException {
        ProtocolFactory pf = new JGroupsProtocolFactory();
        GroupConfiguration gc = new JGroupsGroup("BankSystem");
        service = new JGroupsService();
        Protocol p = pf.createProtocol();
        ControlSession control = p.openControlSession(gc);
        data = p.openDataSession(gc);


        data.setMessageListener(this);
        control.join();
    }

    public int balance() throws IOException {

        replyLock.lock();
        try {
            sendRequest("balance", null);
            //Waits for a reply
            replyCondition.await();

            return (int) reply;

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally{
            replyLock.unlock();
        }

        return -1;

    }

    public boolean movement(int amount) throws IOException {
        replyLock.lock();
        try {
            sendRequest("movement", new String[]{Integer.toString(amount)});
            //Waits for a reply
            replyCondition.await();
            msgId++;

            return (boolean) reply;
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally{
            replyLock.unlock();
        }

        return false;

    }

    @Override
    public Object onMessage(Message message) {
        Packet received = new Packet(message.getPayload());

        replyLock.lock();
        try {
            Object content = received.getContent();


            // If the received message is a reply and the id is the client's...
            // ...continue running the code currently waiting for a reply
            if(!(content instanceof Invocation) && received.getId().equals(buildId())) {
                reply = content;
                replyCondition.signal();
            }

        } finally{
            replyLock.unlock();
        }
        return null;
    }

    public void sendRequest(String request, Object[] args) throws IOException {
        msgId++;
        Invocation i = new Invocation(request, args);
        Packet p = new Packet(buildId(), i);

        Message message = data.createMessage();
        message.setPayload(p.getBytes());
        System.out.println("SENDING:" + request);
        data.multicast(message, service, null);

    }

    public String buildId(){
        return msgId+"@"+clientId;
    }
}
