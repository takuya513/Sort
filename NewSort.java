package sort;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

import quickSort.QuickSort;
import sort.QuickMergeSort_2.QuickSortWorker;

public class NewSort <E extends Comparable> extends QuickSort<E>{
	ExecutorService executor;
	int threadsNum,arrayLength,pos;
	final List<Callable<Object>> workers;
	final LinkedList<MergeInfo> works;
	boolean tmp = false;

	public NewSort() {
		threadsNum = Runtime.getRuntime().availableProcessors();
		executor = Executors.newCachedThreadPool();
		workers = new ArrayList<Callable<Object>>(threadsNum);
		works = new LinkedList<MergeInfo>();
	}
	public void sort(E[] array){
		this.array = array;
		arrayLength = array.length;
		pos = 0;
		if(array[pos].compareTo(array[pos+1]) <= 0)
			ascendingOrder();
		else
			descendindOrder();


		//マージするメソッド
		parallelMergeSort();

		try {
			executor.invokeAll(workers);
		} catch (InterruptedException e) {
			// TODO 自動生成された catch ブロック
			e.printStackTrace();
		}
		executor.shutdown();
	}

	//昇順メソッド
	public void ascendingOrder(){
		int first = pos;
		while(array[pos].compareTo(array[pos+1]) < 0){
			if(pos+1 >= arrayLength-1){
				works.offer(new MergeInfo(first,pos,true));
				return;
			}
			pos++;
		}
		works.offer(new MergeInfo(first,pos,false));
		pos++;
		descendindOrder();
	}

	//降順メソッド
	public void descendindOrder(){

		int first = pos;
		if(pos+1 > arrayLength-1)
			return;

		//どこまで降順かチェック
		while(array[pos].compareTo(array[pos+1]) >= 0){
			pos++;
			if(pos+1 >= arrayLength-1){
				pos++;
				workers.add(Executors.callable(new QuickSortWorker(first,pos)));
				works.offer(new MergeInfo(first,pos,true));
				return;
			}
		}
		if(first != pos){  //二個以上のデータ範囲のとき
			workers.add(Executors.callable(new QuickSortWorker(first,pos)));
		}
		works.offer(new MergeInfo(first,pos,false));
		
		//昇順メソッドへ
		ascendingOrder();  //test
	}

	//マージする部分
	public void parallelMergeSort(){
		//二つのworksを取り出し、left,mid,rightを取り出す
		//最後の一つだった場合
		MergeInfo info1,info2;
		while(true){
			while(true){
				info1 = works.remove();
				if(info1.end() == true)
					break;

				info2 = works.remove();
				workers.add(Executors.callable(new MergeSortWorker(info1.left,info1.right,info2.left-1)));

				if(info1.left == 0 && info2.right == arrayLength-1)  //修正、リストがからっ立ったら
					return;

				works.offer(new MergeInfo(info1.left,info2.right,info2.end()));

				if(info2.end() == true)
					break;
			}

			try {
				executor.invokeAll(workers);
			} catch (InterruptedException e) {
				// TODO 自動生成された catch ブロック
				e.printStackTrace();
			}
		}
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

	class MergeInfo{
		private int left,mid,right;
		private boolean end;
		MergeInfo(int left,int right,boolean end){
			this.left = left;
			this.right = right;
			this.end = end;
		}

		public boolean end(){
			return end;
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
}
