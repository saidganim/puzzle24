package ida.ipl;


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
   public Object data = null; // by default

   public String toString(){
      StringBuilder res = new StringBuilder();
      res.append("MessageObject{messageType");
      if(messageType == JOB_STEALING)
         res.append("JOB_STEALING");
      else if(messageType == JOB_BOARD)
         res.append("JOB_BOARD");
      else
         res.append("SOLUTIONS_NUM");
      res.append("; data:");
      res.append(data.toString());
      res.append(";}");
      return res.toString();
   }
}
