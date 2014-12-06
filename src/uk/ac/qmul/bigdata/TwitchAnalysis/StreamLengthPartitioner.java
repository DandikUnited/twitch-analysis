package uk.ac.qmul.bigdata.TwitchAnalysis;


import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.io.Text;

    public class StreamLengthPartitioner implements Partitioner<Text, Text> {
 
        public int getPartition(Text key, Text value, int numReduceTasks) {
 
            String [] nameAgeScore = value.toString().split("\t");
            String age = nameAgeScore[1];
            int ageInt = Integer.parseInt(age);
           
            //this is done to avoid performing mod with 0
            if(numReduceTasks == 0)
                return 0;
 
            //if the age is <20, assign partition 0
            if(ageInt <=20){               
                return 0;
            }
            //else if the age is between 20 and 50, assign partition 1
            if(ageInt >20 && ageInt <=50){
               
                return 1 % numReduceTasks;
            }
            //otherwise assign partition 2
            else
                return 2 % numReduceTasks;
           
        }

		@Override
		public void configure(JobConf arg0) {
			// TODO Auto-generated method stub
			
		}
    }
 

