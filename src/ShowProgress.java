
public class ShowProgress {
	static int i;
	static int j;
	final static int speed = 15000;

	static long delME = 0;
	
	static void Showl (){
	}
	static void Show (){
		if (j%speed == 0) {
			i++;

			if (i%2 == 0){
				System.out.print('\b');
			} else {
				int k = i/2;
				char c='-';
				switch (k) {
				case 0: c='\\';
				break;
				case 1: c='|';
				break;
				case 2: c='/';
				break;
				case 3: c='-';
				break;
				case 4: c='|';
				break;
				case 5: c='-';
				j=-1;
				i=-1;
				break;
				}
				System.out.print(c);
			}
		}
		
		j++;
	}
	
	static int percent=-1;
	static boolean ShowPercent (int a, int b) {
		boolean ret = false;
		int num = a*100/b;
		
		if (num != percent) {
			ret = true;
			percent = num;
			System.out.print("\b\b\b\b");
			String formatted = String.format("%02d ", num);
			System.out.print(formatted+"%");
		}
		
		return ret;
	}
	
	static int n_x=0;
	static void ShowIntAndIncrement (int N) {
		n_x++;
		ShowPercent(n_x, N);
	}
	static void ShowIntAndIncrementReset () {
		n_x = 0;
	}

	/**
	 * Function prints and flushes display
	 * @param a
	 * @param b
	 */
	static void ShowPercentFlush (int a, int b) {		
		if (ShowPercent(a, b)) {
			System.out.flush();
		}
	}
	
	static void ShowLong (long a, long b) {
		int i_b = size(b);
		int i_a = size(a);
		
		for (int j=0; j<i_b; j++)
			System.out.print("\b");
		
		for (int j=0; j<(i_b-i_a); j++)
			System.out.print(" ");
		
		System.out.print(a);
	}
	
	/**
	 * Display 'a' by first printing 'b' backspaces
	 * @param a
	 */
	static void ShowLong (long a) {
		int i_a = size(a);
		
		for (int j=0; j<i_a; j++)
			System.out.print("\b");
				
		System.out.print(a);
	}
	
	private static int size (long a) {
		int ret = 1;
		
		for (long i=a; i >= 10; ret++)
			i/=10;
		
		return ret;
	}
	public static void ShowLongSample(int k) {
		if(k%100 == 0)
			ShowLong (k);
	}
	
	public static void ShowDouble(Double a, Double b) {
		for (int j=0; j<100; j++)
			System.out.print("\b");
		
		ShowLong(delME++);
		
		int k=0;
		if (b > 0) {
			while (b < 1) {
				k++;
				b *=10;
			}
		}
		System.out.print("#"+a+"#"+k+"#  ");
		
	}
}
