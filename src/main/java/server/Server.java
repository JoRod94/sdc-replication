package server;

import bank.Bank;
import bank.BankImpl;
import bank.BankOperation;

import communication.Invocation;
import communication.Packet;
import data.DataAccess;

import net.sf.jgcs.*;
import net.sf.jgcs.annotation.PointToPoint;
import net.sf.jgcs.jgroups.JGroupsGroup;
import net.sf.jgcs.jgroups.JGroupsProtocolFactory;
import net.sf.jgcs.jgroups.JGroupsService;

import java.io.IOException;
import java.net.SocketAddress;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by joaorodrigues on 14 Apr 16.
 */
public class Server implements MessageListener{
    public static final String GROUP_NAME = "BankSystem";

    private String bankId;
    private Bank bank;
    private int msgId;

    private String dbName;

    // Pending Requests during recovery
    private final Queue<Message> pendingRequests = new LinkedList<>();
    // Indicates whether or not we should ask for the group for the current state
    private boolean recover;
    // Indicates whether or not we are discarding messages
    private boolean discard;

    // Saving the current db connection
    // We need to save it in the server since we must communicate with it
    // for recovery mode logic. Bank implementation should be clear of this logic
    private DataAccess da;

    // JGroups Variables
    private DataSession data;
    private Service service;

    /**
     * Creates a new BankServer
     * If no other BankServer has been created, false should be passed
     * as the argument. Otherwise, the server will enter recovery mode.
     * @param name - database name
     * @param recover - boolean indicating the need for a recovery.
     * @throws IOException
     * @throws InterruptedException
     */
    public Server(String name, boolean recover) throws IOException, InterruptedException, SQLException {
        this.bankId = (new java.rmi.dgc.VMID()).toString();
        this.recover = recover;
        this.dbName = name;

        // If we are in recovery, we must start by discarding
        // If we are not, it doesn't really matter the value of discard
        this.discard = recover;

        // We only create the bank with a brand new database when not recovering
        // Otherwise the bank will be created based on a status update
        if(!recover)
            this.bank = new BankImpl(getDataAccess());
        else
            getDataAccess();

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
     * Obtains a DataAccess object, used to manage the bank database
     * @throws SQLException
     */
    public DataAccess getDataAccess() throws SQLException {
        da = new DataAccess();
        da.initEDBConnection(dbName);
        return da;
    }

    /**
     * Sets the bank state to the received value and processes queued messages
     * @param transactions - set of transactions with ids larger than this bank's id
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private void recover(DataAccess da, ArrayList<BankOperation> transactions) throws IOException, ClassNotFoundException, SQLException {
        this.bank = new BankImpl(da, transactions);

        Message queued;
        while (!pendingRequests.isEmpty()) {
            queued = pendingRequests.remove();
            handle(queued);
        }

        recover = false;
        System.out.println("FINISHED RECOVERY");
    }

    /**
     * Runs the server
     * @throws IOException
     * @throws InterruptedException
     */
    public void work() throws IOException, InterruptedException {
        if(recover) {
            sendRequest(Invocation.STATE, da.getCurrentOperationId());
            System.out.println("STATE REQUEST SENT");
        }

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
    private void handleRecovery(Message m) throws IOException, ClassNotFoundException, SQLException {
        Packet p = new Packet(m.getPayload());
        Object content = p.getContent();

        // If we received an expected message
        if(p.getId().equals(buildPacketId())) {
            // It's either our own first message or a status update
            // If it's our own
            if(content instanceof Invocation) {
                // We stop discarding messages and start saving them
                discard = false;
            } else {
                // If it's by other server, it's a status update
                // We only update the message id here because otherwise we would never
                // get the correct reply. Updating it after we received a
                // correct reply also means we will ignore repeated replies from
                // multiple servers
                msgId++;
                recover(da, (ArrayList<BankOperation>) content);
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
        Object reply = null;

        switch(command) {
            case Invocation.CREATE:
                reply = bank.create();
                break;
            case Invocation.BALANCE:
                reply = bank.balance((String)args[0]);
                break;
            case Invocation.MOVEMENT:
                reply = bank.movement((String)args[0], (int)args[1]);
                break;
            case Invocation.STATE:
                System.out.println("NEW SERVER JOINED. RECEIVED STATE REQUEST");
                reply = getOperationsAfter((int) args[0]);
                System.out.println("RETRIEVED REQUESTED OPERATIONS");
                break;
            case Invocation.TRANSFER:
                reply = bank.transfer( (String) args[0], (String) args[1], (int) args[2]);
                break;
            case Invocation.LATEST:
                reply = bank.latest((String)args[0], (int)args[1]);
                break;
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
    public void sendRequest(String request, Object... args) throws IOException {
        Invocation i = new Invocation(request, args);
        Packet p = new Packet(buildPacketId(), i);

        Message message = data.createMessage();
        message.setPayload(p.getBytes());
        data.multicast(message, service, null);
    }

    private List<BankOperation> getOperationsAfter(int id) {
        return da.getOperationsAfter(id);
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
        } catch (IOException | ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static void main(String[] args){
        try {
            new Server(args[0], Boolean.valueOf(args[1])).work();
        } catch (InterruptedException | IOException | SQLException e) {
            e.printStackTrace();
        }
    }
}
