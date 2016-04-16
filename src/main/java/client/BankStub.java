package client;

import bank.Bank;
import net.sf.jgcs.*;
import net.sf.jgcs.jgroups.JGroupsGroup;
import net.sf.jgcs.jgroups.JGroupsProtocolFactory;
import net.sf.jgcs.jgroups.JGroupsService;
import communication.Packet;
import communication.Invocation;
import server.Server;

import java.io.IOException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by joaorodrigues on 12 Apr 16.
 */
public class BankStub implements Bank, MessageListener {
    private String stubId;
    private int msgId;
    private DataSession data;
    private Service service;

    // Allows to sleep in a condition, until a reply has been received
    private final Lock replyLock = new ReentrantLock();
    private final Condition replyCondition = replyLock.newCondition();

    // Holds the reply to be shared between the invocation and onMessage
    private Object reply;

    public BankStub() throws IOException {
        stubId = (new java.rmi.dgc.VMID()).toString();
        msgId = 0;
        setUpConnection();
    }

    /**
     * Acquires JGroups variables and joins the group for communication
     * @throws GroupException
     */
    private void setUpConnection() throws GroupException {
        ProtocolFactory pf = new JGroupsProtocolFactory();
        GroupConfiguration gc = new JGroupsGroup(Server.GROUP_NAME);
        service = new JGroupsService();
        Protocol p = pf.createProtocol();
        ControlSession control = p.openControlSession(gc);
        data = p.openDataSession(gc);

        data.setMessageListener(this);
        control.join();
    }

    @Override
    public String create(int amount) {
        return (String) invoke(Invocation.CREATE, amount);
    }

    @Override
    public Integer balance(String account) {
        return (Integer) invoke(Invocation.BALANCE, account);
    }

    @Override
    public boolean movement(String account, int amount) {
        return (boolean) invoke(Invocation.MOVEMENT, account, amount);
    }

    @Override
    public boolean transfer(String origin, String destination, int amount) {
        // TODO: implement
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
            if(!(content instanceof Invocation) && received.getId().equals(buildPacketId())) {
                reply = content;
                replyCondition.signal();
            }

        } finally{
            replyLock.unlock();
        }
        return null;
    }

    /**
     * Creates a remote invocation and sends it to all the members of the group
     * @param request - type of invocation to be created. See Invocation class
     * @param args - list of arguments to be sent
     * @throws IOException
     */
    private void sendRequest(String request, Object[] args) throws IOException {
        msgId++;
        Invocation i = new Invocation(request, args);
        Packet p = new Packet(buildPacketId(), i);

        Message message = data.createMessage();
        message.setPayload(p.getBytes());
        System.out.println("SENDING:" + request);
        data.multicast(message, service, null);
    }

    /**
     * Builds the expected packet unique id.
     * The generated id is based on the expected message id
     * and the unique stub id.
     * @return expected packet unique id
     */
    private String buildPacketId() {
        return msgId + "@" + stubId;
    }

    /**
     * Makes a remote method invocation.
     * Since the "message received" invocation is a callback, this method
     * will ensure that the stub sleeps until a reply is received.
     * @param request - type of invocation to be created. See Invocation class
     * @param args - list of arguments to be sent
     * @return - received reply from the server
     */
    private Object invoke(String request, Object... args) {
        // Acquire the replyLock.
        // This will allow the stub to sleep until a reply has been received
        replyLock.lock();
        try {
            sendRequest(request, args);
            replyCondition.await(); // Sleep until a reply has arrived
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally{
            replyLock.unlock();
        }

        // After sleeping, the reply will be available in the reply variable
        return reply;
    }
}
