package server;

import bank.Bank;
import net.sf.jgcs.*;
import net.sf.jgcs.annotation.PointToPoint;
import net.sf.jgcs.jgroups.JGroupsGroup;
import net.sf.jgcs.jgroups.JGroupsProtocolFactory;
import net.sf.jgcs.jgroups.JGroupsService;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import communication.Invocation;
import communication.Packet;

/**
 * Created by joaorodrigues on 14 Apr 16.
 */
public class BankServer implements MessageListener{
    public static final String GROUP_NAME = "BankSystem";

    private String bankId;
    private Bank bank;
    private int msgId;

    // Pending Requests during recovery
    private final PriorityQueue<Message> pendingRequests = new PriorityQueue<>();
    // Indicates whether or not we should ask for the group for the current state
    private boolean recover;
    // Indicates whether or not we are discarding messages
    private boolean discard;

    // JGroups Variables
    private DataSession data;
    private Service service;

    /**
     * Creates a new BankServer
     * If no other BankServer has been created, false should be passed
     * as the argument. Otherwise, the server will enter recovery mode.
     * @param recover - boolean indicating the need for a recovery.
     * @throws IOException
     * @throws InterruptedException
     */
    public BankServer(boolean recover) throws IOException, InterruptedException {
        this.bankId = (new java.rmi.dgc.VMID()).toString();
        this.recover = recover;

        // If we are in recovery, we must start by discarding
        // If we are not, it doesn't really matter the value of discard
        this.discard = recover;

        // We only create the bank if we won't go into recovery.
        // Otherwise the bank will be received
        if(!recover)
            this.bank = new BankImpl();

        setUpConnection();
    }

    /**
     * Acquires JGroups variables and joins the group for communication
     */
    public void setUpConnection() throws GroupException {
        ProtocolFactory pf = new JGroupsProtocolFactory();
        GroupConfiguration gc = new JGroupsGroup(GROUP_NAME);
        service = new JGroupsService();
        Protocol p = pf.createProtocol();
        ControlSession control = p.openControlSession(gc);
        data = p.openDataSession(gc);


        data.setMessageListener(this);
        control.join();
    }

    /**
     * Sets the bank state to the received value and processes queued messages
     * @param value - new bank state
     * @throws IOException
     * @throws ClassNotFoundException
     */
    // TODO: Change this to the correct class
    private void recover(BankImpl value) throws IOException, ClassNotFoundException {
        this.bank = new BankImpl(value);

        Message queued;
        while (!pendingRequests.isEmpty()) {
            queued = pendingRequests.remove();
            handle(queued);
        }

        recover = false;
    }

    /**
     * Runs the server
     * @throws IOException
     * @throws InterruptedException
     */
    public void work() throws IOException, InterruptedException {
        if(recover)
            sendRequest(Invocation.STATE, null);

        // Waits in a non-blocking manner forever
        while(true){
            try {
                Thread.sleep(100000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Handle a message in recovery mode.
     * Decides if we should update our state, save the message or discard it
     * @param m - received message
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private void handleRecovery(Message m) throws IOException, ClassNotFoundException {
        Packet p = new Packet(m.getPayload());
        Object content = p.getContent();

        System.out.println("INTERPRETING: " + content.toString());

        // If we received an expected message
        if(p.getId().equals(buildPacketId())) {
            // It's either our own first message or a status update
            // If it's our own
            if(content instanceof Invocation) {
                // We stop discarding messages and start saving them
                discard = false;
            } else {
                // If it's by other server, it's a status update
                // TODO: cast to correct class
                recover((BankImpl)content);
            }
        } else {
            // If we received an unexpected message
            // We either save it or discard it
            if(!discard)
                pendingRequests.add(m);
        }
    }

    /**
     * Makes the interface between a remote invocation and the underlying bank
     * @param command - command to be invoked
     * @param args - command arguments
     * @return
     */
    private Object handleInvocation(String command, Object[] args) {
        Object reply;

        switch(command) {
            case Invocation.CREATE:
                reply = bank.create((Integer)args[0]);
                break;
            case Invocation.BALANCE:
                reply = bank.balance((String)args[0]);
                break;
            case Invocation.MOVEMENT:
                reply = bank.movement((String)args[0], (int)args[1]);
                break;
            case Invocation.STATE:
                // TODO: Update this to the recovery params
                reply = bank;
                break;
            case Invocation.TRANSFER:
                // TODO
            default:
                reply = null;
        }

        return reply;
    }

    /**
     * Handles a message when not in recovery
     * @param m - Received message
     */
    private void handle(Message m) throws IOException {
        Packet p = new Packet(m.getPayload());
        Object o = p.getContent();

        // This method is only invoked when we are not in recovery
        // So, we only handle invocations.
        // This prevents that we process repeated replies
        // for our recovery request
        if(o instanceof Invocation){
            Invocation i = (Invocation)o;
            Object attachment = handleInvocation(i.getCommand(), i.getArgs());
            reply(new Packet(p.getId(), attachment), m.getSenderAddress());
        }
    }

    /**
     * Sends a packet directly to a given destination
     * @param p - Packet to be sent
     * @param destination - Address of the destination
     * @throws IOException
     */
    private void reply(Packet p, SocketAddress destination) throws IOException {
        Message m = data.createMessage();
        m.setPayload(p.getBytes());
        data.multicast(m, service, new PointToPoint(destination));
    }

    /**
     * Creates a remote invocation and sends it to all the members of the group
     * @param request - type of invocation to be created. See Invocation class
     * @param args - list of arguments to be sent
     * @throws IOException
     */
    public void sendRequest(String request, Object[] args) throws IOException {
        Invocation i = new Invocation(request, args);
        Packet p = new Packet(buildPacketId(), i);

        Message message = data.createMessage();
        message.setPayload(p.getBytes());
        data.multicast(message, service, null);
        msgId++;
    }

    /**
     * Builds the expected packet unique id.
     * The generated id is based on the expected message id
     * and the unique stub id.
     * @return expected packet unique id
     */
    public String buildPacketId(){
        return msgId + "@" + bankId;
    }

    @Override
    public Object onMessage(Message message) {
        try {
            if(recover)
                handleRecovery(message);
            else
                handle(message);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static void main(String[] args){
        try {
            new BankServer(Boolean.getBoolean(args[0])).work();
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}
