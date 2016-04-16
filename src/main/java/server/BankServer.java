package server;

import net.sf.jgcs.*;
import net.sf.jgcs.annotation.PointToPoint;
import net.sf.jgcs.jgroups.JGroupsGroup;
import net.sf.jgcs.jgroups.JGroupsProtocolFactory;
import net.sf.jgcs.jgroups.JGroupsService;

import java.io.IOException;
import java.net.SocketAddress;
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

    private String serverId;
    private Bank bank;
    private boolean recovered;
    private boolean waitingRecoveryReply;
    private int msgId;

    // Pending Requests during recovery
    private final PriorityQueue<Message> pendingRequests = new PriorityQueue<>();

    // JGroups Variables
    private DataSession data;
    private Service service;

    // Condition control for replies
    private final Lock replyLock = new ReentrantLock();
    private final Condition replyCondition = replyLock.newCondition();



    public BankServer(Bank bank) throws IOException, InterruptedException {
        this.serverId = (new java.rmi.dgc.VMID()).toString();
        this.bank = bank;

        setUpConnection();

        recovered = false;
        waitingRecoveryReply = false;
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

    public void work() throws IOException, InterruptedException {
        recover();

        // Waits in a non-blocking manner forever
        while(true){
            try {
                Thread.sleep(100000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    private void interpret(Message message) throws IOException, ClassNotFoundException {


        Packet receivedPacket = new Packet(message.getPayload());
        Object receivedContent = receivedPacket.getContent();

        System.out.println("INTERPRETING: " + receivedContent.toString());

        // Recovered = Typical workflow
        if(recovered && !receivedPacket.getId().equals(buildId()) && receivedContent instanceof Invocation)
            handleRequest((Invocation) receivedContent, receivedPacket.getId(), message.getSenderAddress());

        if(!recovered){
            // If this server hasn't recovered yet...
            // And if the packet came from this server...
            if(receivedPacket.getId().equals(buildId())){

                // If the packet is a request, the packet indicates that the recovery requests have been sent...
                // ...and the server now waits for recovery replies
                if(receivedContent instanceof Invocation)
                    waitingRecoveryReply = true;
                else{
                    // Otherwise, it's a recovery reply, a status update
                    handleRecoveryReply(receivedContent);
                    waitingRecoveryReply = false;
                    handlePendingRequests();
                }
            }
            else if(waitingRecoveryReply) {
                // If it's an external request, and the server is waiting for a status update...
                // ...add to pending requests, as these will have to be handled after the update
                pendingRequests.add(message);
            }
        }
    }

    public void handleRequest(Invocation invocation, String packetId, SocketAddress senderAddress) throws IOException {
        Message sent = data.createMessage();

        Object[] args = invocation.getArgs();
        Packet reply = null;

        switch(invocation.getCommand()){
            case "balance":
                //TODO: Insert correct bank call
                reply = new Packet(packetId, 1337);
                //reply = new Packet(packetId,bank.balance());
                break;
            case "movement":
                //TODO: Insert correct bank call
                //reply = new Packet(packetId, bank.movement(args[0]));
                break;
        }

        //Send the reply if it was created
        if(reply != null) {
            sent.setPayload(reply.getBytes());
            data.multicast(sent, service, new PointToPoint(senderAddress));
        }
    }

    public void handlePendingRequests() throws IOException, ClassNotFoundException {
        Message queued;
        while (!pendingRequests.isEmpty()) {
            queued = pendingRequests.remove();
            interpret(queued);
        }
    }

    public void handleRecoveryReply(Object reply){
        replyLock.lock();
        try {

            // TODO: Recovery Operations

            replyCondition.signal();
        }finally{
            replyLock.unlock();
        }
    }

    public void recover() throws IOException, InterruptedException {
        replyLock.lock();
        try {

            //TODO: Add correct recover operations
            sendRequest("balance", null);

            System.out.println("WAITING FOR RECOVERY REPLY...");

            // Waits for onMessage to continue the execution
            // If there's a timeout, it probably means the server is alone
            replyCondition.await(5, TimeUnit.SECONDS);
            System.out.println("WAITING FINISHED");

            recovered = true;
        }finally {
            replyLock.unlock();
        }

    }

    public void sendRequest(String request, Object[] args) throws IOException {
        Invocation i = new Invocation(request, args);
        Packet p = new Packet(buildId(), i);

        Message message = data.createMessage();
        message.setPayload(p.getBytes());
        data.multicast(message, service, null);
        msgId++;
    }

    public String buildId(){
        return msgId+"@"+serverId;
    }

    @Override
    public Object onMessage(Message message) {
        try {
            interpret(message);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        return null;
    }


    public static void main(String[] args){
        try {
            (new BankServer( new Bank() )).work();
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }


}
