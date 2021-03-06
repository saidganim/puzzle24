package ida.ipl;
import ibis.ipl.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Ida implements MessageUpcall{

    // Level of deepness master node should go in for generating the jobs
    static int MAXHOPS = 2;

    private Ibis myIbis;
    private List<Board> masterJobsList;
    private Boolean jobListBusy = false;
    private int solutionsNum = 0;
    long jobCounter = 0;
    long solutionsStep = Integer.MAX_VALUE;
    long startTime;
    long endTime;

    public Ida(String[] args) throws Exception {
        String fileName = null;
        boolean cache = true;
        int length = 103;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--nocache")) {
                cache = false;
            }
        }
        myIbis = IbisFactory.createIbis(ibisCapabilities, null,
            requestPortType, replyPortType);
        IbisIdentifier server = myIbis.registry().elect("Server");

        if (server.equals(myIbis.identifier())) {
            for (int i = 0; i < args.length; i++) {
                if (args[i].equals("--file")) {
                    fileName = args[++i];
                } else if (args[i].equals("--length")) {
                    i++;
                    length = Integer.parseInt(args[i]);
                } else if(!args[i].equals("--nocache")){
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
            masterNode(initialBoard, cache);
        } else {
            slaveNode(server, cache);
        }
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
            PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_EXPLICIT,
            PortType.CONNECTION_MANY_TO_ONE);

    IbisCapabilities ibisCapabilities = new IbisCapabilities(
            IbisCapabilities.ELECTIONS_STRICT);


    private int solutions(Board board, BoardCache cache) {
        if (board.distance() == 0)
            return 1;

        if (board.distance() > board.bound() || board.distance() > solutionsStep)
            return 0;
        Board[] children;
        if(cache == null)
            children = board.makeMoves();
        else
            children = board.makeMoves(cache);
        int result = 0;

        for (int i = 0; i < children.length; i++) {
            if (children[i] != null) {
                result += solutions(children[i], cache);
            }
        }
        cache.put(children);
        return result;
    }

    private Pair<Integer, Integer> solve(Board board, boolean useCache) {
        BoardCache cache = null;
        if (useCache) {
            cache = new BoardCache();
        }
        int bound = board.distance();
        int solutions = 0;
        System.out.print("Try bound ");
        System.out.flush();

        do {
            System.out.print(bound + " ");
            System.out.flush();

            board.setBound(bound);
            if (useCache) {
                solutions = solutions(board, cache);
            } else {
                solutions = solutions(board, null);
            }
            bound += 2;
        } while (solutions == 0 || bound > solutionsStep);
        return new Pair<Integer, Integer>(solutions, board.bound());
    }


    @Override
    public void upcall(ReadMessage message) throws IOException, ClassNotFoundException {
        // Notify Master node main thread that all work is done
	    MessageObject readMessage = (MessageObject) message
                .readObject();
        message.finish();
    	ReceivePortIdentifier requestor = readMessage.requestor;
        MessageObject response = new MessageObject();
        if(requestor == null)
	    	return;
        synchronized (masterJobsList){
            if(readMessage.messageType == MessageObject.message_id.JOB_STEALING){
                // Provide slave with one another job
                synchronized (jobListBusy){
                    if(masterJobsList.size() > 0){
                        response.messageType = MessageObject.message_id.JOB_BOARD;
                        response.data = masterJobsList.get(0);
                        masterJobsList.remove(0);
                        response.maximumBound = solutionsStep;
                    }
                }

                SendPort replyPort = myIbis.createSendPort(replyPortType);
                replyPort.connect(requestor);
                WriteMessage reply = replyPort.newMessage();
                reply.writeObject((response));
                reply.finish();
                replyPort.close();


            } else if (readMessage.messageType == MessageObject.message_id.SOLUTIONS_NUM){
                --jobCounter;
                Pair<Integer, Integer> res = (Pair<Integer, Integer>)readMessage.data;
                System.out.println("GOT RESULT (" + res.getKey() + " ; " + res.getValue() + ")");
                synchronized(jobListBusy){
                    if(res.getValue() < solutionsStep){
                        solutionsNum = res.getKey();
                        solutionsStep = res.getValue();
                        endTime = System.currentTimeMillis();
                    } else if (res.getValue() == solutionsStep){
                        solutionsNum += res.getKey();
                        endTime = System.currentTimeMillis();
                    } else {
                        // do nothing
                    }
                    if(jobCounter == 0)
                        jobListBusy.notify();
                }

            }

        }
    }

    public void masterNode(Board initState, boolean useCache) throws Exception {
        // Master Node should provide with jobs
        ReceivePort receiver = myIbis.createReceivePort(requestPortType,
                "server", this);

        synchronized (jobListBusy){
            // enable connections
            receiver.enableConnections();
            // enable upcalls
            receiver.enableMessageUpcalls();
            masterJobsList = getjobs(initState, useCache);
            jobCounter = masterJobsList.size();
            startTime = System.currentTimeMillis();
            while(masterJobsList.size() > 0)
                jobListBusy.wait();
        }
        System.err.println("Job is done. Solutions number = " + solutionsNum + "; Time spent on task is " + (endTime - startTime));

    }


    public void slaveNode(IbisIdentifier masterNode, boolean useCache) throws Exception{
        SendPort sendPort = myIbis.createSendPort(requestPortType);
        sendPort.connect(masterNode, "server");
        ReceivePort receivePort = myIbis.createReceivePort(replyPortType, null);
        receivePort.enableConnections();
        WriteMessage request = sendPort.newMessage();
        MessageObject jobRequest = new MessageObject();
        jobRequest.messageType = MessageObject.message_id.JOB_STEALING;
        jobRequest.requestor = receivePort.identifier();
        request.writeObject(jobRequest);
        request.finish();
        MessageObject localSolutionResult = new MessageObject();
        localSolutionResult.messageType = MessageObject.message_id.SOLUTIONS_NUM;
        localSolutionResult.requestor = receivePort.identifier();
        ReadMessage reply = receivePort.receive();
        MessageObject job = (MessageObject)reply.readObject();
        reply.finish();
        while(job.messageType == MessageObject.message_id.JOB_BOARD){
            if(job.data == null){
                sendPort.close();
                receivePort.close();
                return;
            }
	        Board initState = (Board)job.data;
            solutionsStep = job.maximumBound;
            Pair<Integer, Integer> res = solve(initState,useCache);
            System.out.println("SLAVE NODE  SOLVED ONE "  + res.getKey() + " :: " + res.getValue());
            localSolutionResult.data = res;
            request = sendPort.newMessage();
            request.writeObject(localSolutionResult);
            request.finish();

            request = sendPort.newMessage();
            request.writeObject(jobRequest);
            request.finish();
            reply = receivePort.receive();
            job = (MessageObject)reply.readObject();
            reply.finish();
        }

        sendPort.close();
        receivePort.close();
    }

    private List<Board> getjobs(Board boardState, boolean useCache){

        return useCache? __getjobs(boardState, MAXHOPS, new BoardCache()) : __getjobs(boardState, MAXHOPS, null);
    }

    private List<Board> __getjobs(Board boardState, int deepLevel, BoardCache cache){
        ArrayList<Board> result = new ArrayList<Board>();
        boardState.setBound(boardState.distance());
        if(deepLevel == 0){
            Board[] children;
            if(cache == null)
                 children = boardState.makeMoves();
            else
                children = boardState.makeMoves(cache);

            for (int i = 0; i < children.length; i++) {
                if (children[i] != null) {
                    result.add(children[i]);
                    children[i].dropParams();
                }
            }
        } else {
            Board[] children = boardState.makeMoves(cache);
            for (int i = 0; i < children.length; i++) {
                if (children[i] != null) {
                    result.addAll(__getjobs(children[i], deepLevel - 1, cache));
                }
            }
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        new Ida(args);

    }
}
