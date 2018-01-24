package ida.ipl;

import ibis.ipl.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Ida implements MessageUpcall{

    // Level of deepness master node should go in for generating the jobs
    static int MAXHOPS = 10;

    private Ibis myIbis;
    private List<Board> masterJobsList;
    private Boolean jobListBusy = false;
    private int solutionsNum = 0;

    public Ida(Board initialBoard, boolean useCache) throws Exception {
        // Create an ibis instance.
        // Notice createIbis uses varargs for its parameters.
        myIbis = IbisFactory.createIbis(ibisCapabilities, null,
                requestPortType, replyPortType);

        // Elect a server
        IbisIdentifier server = myIbis.registry().elect("Server");

        // If I am the server, run server, else run client.
        if (server.equals(myIbis.identifier())) {
            masterNode(initialBoard);
        } else {
            slaveNode(server);
        }

        // End ibis.
        myIbis.end();
    }
    /**
     * Port type used for sending a request to the server
     */
    PortType requestPortType = new PortType(PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_AUTO_UPCALLS,
            PortType.CONNECTION_MANY_TO_ONE);

    /**
     * Port type used for sending a reply back
     */
    PortType replyPortType = new PortType(PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_DATA, PortType.RECEIVE_EXPLICIT,
            PortType.CONNECTION_ONE_TO_ONE);

    IbisCapabilities ibisCapabilities = new IbisCapabilities(
            IbisCapabilities.ELECTIONS_STRICT);

    private static int solutions(Board board) {
        if (board.distance() == 0) {
            return 1;
        }

        if (board.distance() > board.bound()) {
            return 0;
        }

        Board[] children = board.makeMoves();
        int result = 0;

        for (int i = 0; i < children.length; i++) {
            if (children[i] != null) {
                result += solutions(children[i]);
            }
        }
        return result;
    }

    @Override
    public void upcall(ReadMessage message) throws IOException, ClassNotFoundException {
        MessageObject readMessage = (MessageObject) message
                .readObject();

        System.err.println("received request from: " + readMessage);
        message.finish();
        MessageObject response = new MessageObject();
        if(readMessage.messageType == MessageObject.message_id.JOB_STEALING){
            // Provide slave with one another job
            synchronized (jobListBusy){
                if(masterJobsList.size() > 0){
                    response.messageType = MessageObject.message_id.JOB_BOARD;
                    response.data = masterJobsList.get(0);
                    masterJobsList.remove(0);
                }
            }
        } else if (readMessage.messageType == MessageObject.message_id.SOLUTIONS_NUM){
            // Accept solution number from slave node
            solutionsNum += (Integer)readMessage.data;
            if(masterJobsList.size() == 0)
                jobListBusy.notify(); // Notify Master node main thread that all work is done
        }

    }

    public void masterNode(Board initState) throws Exception {
        // Master Node should provide with jobs
        ReceivePort receiver = myIbis.createReceivePort(requestPortType,
                "server", this);

        synchronized (jobListBusy){
            // enable connections
            receiver.enableConnections();
            // enable upcalls
            receiver.enableMessageUpcalls();
            masterJobsList = getjobs(initState);
            while(masterJobsList.size() != 0)
                jobListBusy.wait();
        }

        System.err.println("Job is done. Solutions number = " + solutionsNum);

        System.err.println("Master Node is started");
    }


    public void slaveNode(IbisIdentifier masterNode) throws Exception{
        SendPort sendPort = myIbis.createSendPort(requestPortType);
        sendPort.connect(masterNode, "server");

        ReceivePort receivePort = myIbis.createReceivePort(replyPortType, null);
        receivePort.enableConnections();

        WriteMessage request = sendPort.newMessage();
        MessageObject jobRequest = new MessageObject();
        jobRequest.messageType = MessageObject.message_id.JOB_STEALING;
        request.writeObject(jobRequest);
        request.finish();

        MessageObject localSolutionResult = new MessageObject();
        localSolutionResult.messageType = MessageObject.message_id.SOLUTIONS_NUM;

        ReadMessage reply = receivePort.receive();
        MessageObject job = (MessageObject)reply.readObject();
        reply.finish();

        while(job.messageType == MessageObject.message_id.JOB_BOARD){
            Board initState = (Board)job.data;
            // handling board
            localSolutionResult.data = (Integer)solutions(initState);
            // Sending given result
            request = sendPort.newMessage();
            request.writeObject(localSolutionResult);
            request.finish();

            // Send new request for job stealing and get result
            request = sendPort.newMessage();
            request.writeObject(jobRequest);
            request.finish();
            reply = receivePort.receive();
            job = (MessageObject)reply.readObject();
            reply.finish();
        }

        // Close ports.
        sendPort.close();
        receivePort.close();
    }

//    public void run(Board initState) throws Exception {
//// Create an ibis instance.
//        Ibis ibis = IbisFactory.createIbis(ibisCapabilities, null, requestPortType, replyPortType);
//
//        // Elect a server
//        IbisIdentifier server = ibis.registry().elect("Server");
//
//        // If I am the server, run server, else run client.
//        if (server.equals(ibis.identifier())) {
//            masterNode(ibis, initState);
//        } else {
//            slaveNode(ibis, server);
//        }
//
//        // End ibis.
//        ibis.end();
//    }


    private List<Board> getjobs(Board boardState){
        return __getjobs(boardState, MAXHOPS);
    }

    private List<Board> __getjobs(Board boardState, int deepLevel){
        ArrayList<Board> result = new ArrayList<Board>();
        if(deepLevel == 0){
            Board[] children = boardState.makeMoves();
            for (int i = 0; i < children.length; i++) {
                if (children[i] != null) {
                    result.add(children[i]);
                }
            }
        } else {
            Board[] children = boardState.makeMoves();
            for (int i = 0; i < children.length; i++) {
                if (children[i] != null) {
                    result.addAll(__getjobs(children[i], deepLevel - 1));
                }
            }
        }
        return result;
    }


    public static void main(String[] args) throws Exception {
        String fileName = null;
        boolean cache = true;
        int length = 103;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--file")) {
                fileName = args[++i];
            } else if (args[i].equals("--nocache")) {
                cache = false;
            } else if (args[i].equals("--length")) {
                i++;
                length = Integer.parseInt(args[i]);
            } else {
                System.err.println("No such option: " + args[i]);
                System.exit(1);
            }
        }

        Board initialBoard = null;

        if (fileName == null) {
            initialBoard = new Board(length);
        } else {
            try {
                initialBoard = new Board(fileName);
            } catch (Exception e) {
                System.err
                        .println("could not initialize board from file: " + e);
                System.exit(1);
            }
        }
        System.out.println("Running IDA*, initial board:");
        System.out.println(initialBoard);
        new Ida(initialBoard, cache);

    }
}
