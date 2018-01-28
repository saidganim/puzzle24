package ida.ipl;


import ibis.ipl.ReceivePortIdentifier;

import java.io.Serializable;

import static ida.ipl.MessageObject.message_id.*;

/**
 * Class which represents message object
 * Should be used to be sent from client to server
 * Can possibly contain Job stealing request or returning value from one job execution
 */
public class MessageObject implements Serializable{
   enum message_id{ JOB_STEALING, SOLUTIONS_NUM, JOB_BOARD, EMPTY_MESSAGE};

   public message_id messageType = EMPTY_MESSAGE; // by default
   public Serializable data = null; // by default
   public ReceivePortIdentifier requestor;

   public String toString(){
      StringBuilder res = new StringBuilder();
      res.append("MessageObject{messageType: ");
      if(messageType == JOB_STEALING)
         res.append("JOB_STEALING");
      else if(messageType == JOB_BOARD)
         res.append("JOB_BOARD");
      else
         res.append("SOLUTIONS_NUM");
      res.append("; data:");
      
	if(data != null)
		res.append(data.toString());
	else
		res.append("null");
      res.append(";}");
      return res.toString();
   }
}
