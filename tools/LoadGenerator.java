import java.io.*;

public class LoadGenerator
{
    public static void main(String[] args) throws Exception {

	if(args.length!=1)
	    {
		System.out.println("Usage: LoadGenerator <events per second>");
		System.exit(1);
	    }

	// The number of events to generate per second
	int rate = Integer.parseInt(args[0]);

	System.out.println("Rate of generation: " + rate);

	/* 
	   The interval between each event
	   Calculated by splicing each second into 'rate' nanoseconds
	*/
	int interval = (int) (Math.pow(10, 9) / rate);

	// The file with events, which can be read by Flume
	PrintStream writer = System.out;

	// Each iteration in the while loop runs for approximately one second
	while(!Thread.interrupted())
	    {
		long start = System.nanoTime(), end = 0, check;
		long begin = start;
		long count = 0;
		for (int i = 0; i < rate; i++) {
		    // Write event to file
		    writer.println("Harry Potter is a series of seven fantasy novels written by the British author J. K. Rowling. The books chronicle the adventures of the adolescent wizard Harry Potter and his best friends Ron Weasley and Hermione Granger, all of whom are students at Hogwarts School of Witchcraft and Wizardry. The main story arc concerns Harry's quest to overcome the evil dark wizard Lord Voldemort, whose aim is to subjugate non-magical people, conquer the wizarding world, and destroy all those who stand in his way, especially Harry Potter.Harry Potter is a series of seven fantasy novels written by the British author J. K. Rowling. The books chronicle the adventures of the adolescent wizard Harry Potter and his best friends Ron Weasley and Hermione Granger, all of whom are students at Hogwarts School of Witchcraft and Wizardry. The main story arc concerns Harry's quest to overcome the evil dark wizard Lord Voldemort, whose aim is to subjugate non-magical people, conquer the wizarding world, and destroy all those who stand in his way, especially Harry Potter.Harry Potter is a series of seven fantasy novels written by the British author J. K. Rowling. The books chronicle the adventures of the adolescent wizard Harry Potter and his best friends Ron Weasley and Hermione Granger, all of whom are students at Hogwarts School of Witchcraft and Wizardry. The main story arc concerns Harry's quest to overcome the evil dark wizard Lord Voldemort, whose aim is to subjugate non-magical people, conquer the wizarding world, and destroy all those who stand in his way, especially Harry Potter.Harry Potter is a series of seven fantasy novels written by the British author J. K. Rowling. The books chronicle the adventures of the adolescent wizard Harry Potter and his best friends Ron Weasley and Hermione Granger, all of whom are students at Hogwarts School of Witchcraft and Wizardry. The main story arc concerns Harry's quest to overcome the evil dark wizard Lord Voldemort, whose aim is to subjugate non-magical people, conquer the wizarding world, and destroy all those who stand in his way, especially Harry Potter.Harry Potter is a series of seven fantasy novels written by the British author J. K. Rowling. The books chronicle the adventures of the adolescent wizard Harry Potter and his best friends Ron Weasley and Hermione Granger, all of whom are students at Hogwarts School of Witchcraft and Wizardry. The main story arc concerns Harry's quest to overcome the evil dark wizard Lord Voldemort, whose aim is to subjugate non-magical people, conquer the wizarding world, and destroy all those who stand in his way, especially Harry Potter.Harry Potter is a series of seven fantasy novels written by the British author J. K. Rowling. The books chronicle the adventures of the adolescent wizard Harry Potter and his best friends Ron Weasley and Hermione Granger, all of whom are students at Hogwarts School of Witchcraft and Wizardry. The main story arc concerns Harry's quest to overcome the evil dark wizard Lord Voldemort, whose aim is to subjugate non-magical people, conquer the wizarding world, and destroy all those who stand in his way, especially Harry Potter.: " + i);
		    /*
		      Find and wait for the end of this timeslice
		      This is the fastest way in Java
		      There is a Thread.sleep(millis, nanos) method but it has an extremely high overhead
		    */
		    check = start + interval;
		    do {
			end = System.nanoTime();
		    } while (check >= end);		    
		    start = System.nanoTime();
		}
		System.out.println(rate + " events generated in " + ((double) (end - begin) / 1000000000) + " seconds");
	    }

	writer.close();
    }
}

