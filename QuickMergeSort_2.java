package sort;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import quickSort.QuickSort;
import tools.MyArrayUtil;
import tools.MyData;
import tools.MyInteger;

public class QuickMergeSort_2<E extends Comparable> extends QuickSort<E> {
	ExecutorService executor;
	int threadsNum, arrayLength, sectionOfSort, pivotOfEnd, pos, pos2;
	int pivotOfRest = -1; //マージするときに余った部分の仕切り
	final ArrayList<Callable<Object>> workers;

	public QuickMergeSort_2(){
		threadsNum = Runtime.getRuntime().availableProcessors()-1;
		//threadsNum =2;
		executor = Executors.newFixedThreadPool(threadsNum);
		workers = new ArrayList<Callable<Object>>(threadsNum);

	}

	public void sort(E[] array){
		this.array = array;
		arrayLength = array.length;
		sectionOfSort = array.length / threadsNum;

		pivotOfEnd = 0; pos = 0;  pos2 = sectionOfSort - 1;

		parallelQuickSort();

		//ここからマージ処理を行う

		try {
			parallelMergeSort();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}


	}

	private void parallelMergeSort() throws InterruptedException {
		int expansionSection = 2;
		int lastLenOfRestSection = -1;  //繰り返し防止を確認するための変数
		pos = 0;pos2 = sectionOfSort * expansionSection - 1 ;
		while(pos2 <= arrayLength - 1){
			workers.clear();
			threadsNum = threadsNum/2+1;
			((ThreadPoolExecutor)executor).setCorePoolSize(threadsNum);

			while(true){
				workers.add(Executors.callable(new MergeSortWorker(pos,pos2)));
				if(pos2 == arrayLength-1)
					break;

				pos = pos2 + 1;
				pivotOfEnd = pos;
				pos2 = pos2 + sectionOfSort*expansionSection;

				//余り部分の処理
				if(pos2 > arrayLength-1){
					if(cheakSameSort(arrayLength,pos,lastLenOfRestSection)) break;
					if(pivotOfRest == -1)  //最初のあまり分のとき
						workers.add(Executors.callable(new MergeSortWorker(pos,arrayLength - 1)));
					else
						workers.add(Executors.callable(new MergeSortWorker(pos,pivotOfRest-1,arrayLength - 1)));

					pivotOfRest = pos;
					lastLenOfRestSection = arrayLength - pos;
					break;
				}
			}

			expansionSection = expansionSection * 2;
//			System.out.println("threadsNum : "+threadsNum);
//			System.out.println(workers.size());
			executor.invokeAll(workers);
			pos = 0;pos2 = sectionOfSort * expansionSection - 1 ;

			//System.out.println(((ThreadPoolExecutor)executor).getCorePoolSize());
		}
		executor.shutdown();
		//最後のmerge
		merge(0,pivotOfEnd-1,arrayLength - 1,new LinkedList<E>());
	}


	private void parallelQuickSort(){
		//クイックソートをする
		while(pos2 < arrayLength){
			workers.add(Executors.callable(new QuickSortWorker(pos,pos2)));
			pos = pos2 + 1;
			pos2 =  sectionOfSort + pos2;
		}

		//最後の区分だけ特別に処理する
		workers.add(Executors.callable(new QuickSortWorker(pos,arrayLength-1)));
		pivotOfRest = pos;

		try {
			executor.invokeAll(workers);
		} catch (InterruptedException e) {
			// TODO 自動生成された catch ブロック
			e.printStackTrace();
		}

	}

	//前回同じ範囲でソートしたかcheak
	public boolean cheakSameSort(int arrayLength,int pos,int lastLenOfRestSection){
		if((arrayLength - pos) == lastLenOfRestSection)
			return true;

		return false;
	}

	public  void merge(int left,int mid,int right,LinkedList<E> buff){
		int i = left,j = mid + 1;

		while(i <= mid && j <= right) {
			if(array[i].compareTo(array[j]) < 0){
				buff.add(array[i]); i++;
			}else{
				buff.add(array[j]); j++;
			}
		}

		while(i <= mid) { buff.add(array[i]); i++;}
		while(j <= right) { buff.add(array[j]); j++;}
		for(i = left;i <= right; i++){ array[i] = buff.remove(0);}
	}


	class MergeSortWorker implements Runnable{
		int left,right,mid;
		LinkedList<E> buff;
		public MergeSortWorker(int left,int right){
			this.left = left;
			this.right = right;
			mid = (left + right) / 2;
			buff = new LinkedList<E>();
		}

		public MergeSortWorker(int left,int mid,int right){
			this.left = left;
			this.right = right;
			this.mid = mid;
			buff = new LinkedList<E>();
		}
		public void run(){
			merge(left,mid,right,buff);
		}
	}

	class QuickSortWorker implements Runnable {
		int left,right;
		public QuickSortWorker(int left,int right){
			this.left = left;
			this.right = right;
		}

		public void run() {
			quickSort(left,right);
		}
	}




	//	public  void quickSort(int left,int right){
	//		super.quickSort(left, right);
	//	}

}
