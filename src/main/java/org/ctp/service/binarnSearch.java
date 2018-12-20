package org.ctp.service;

public class binarnSearch {


    public static Integer binSearch(Integer[] arr,Integer begin,Integer end,Integer target){
        if(arr.length == 0){
            return null;
        }
        if(begin > end){
            return null;
        }
        int mid = (begin + end) / 2;
        if(target.equals(arr[mid])){
            return mid;
        }
        else if(target < arr[mid]){
            return binSearch(arr,begin,mid -1,target);
        }
        else {
            return binSearch(arr,mid+1,end,target);
        }

    }



    public static Integer binSearch2(Integer[] arr,Integer begin,Integer end,Integer target){
        Integer result = null;
        if(arr.length == 0){
            return null;
        }
        while(begin <= end){
            int mid = (begin + end) / 2;
            if(target.equals(arr[mid])){
                result = mid;
                break;
            }
            else if(target < arr[mid]){
                end = mid - 1;
            }
            else{
                begin = mid + 1;
            }
        }
        return result;

    }


    public static Integer binSearch3(Integer[] arr,Integer begin,Integer end,Integer target){
            Integer result = null;
            if(arr.length == 0){
                return null;
            }
            while(begin <= end){
                int mid = (begin + end) / 2;
                if(target.equals(arr[mid])){
                    result = mid;
                    break;
                }
                else if(target < arr[mid]){
                    if(target < arr[begin]) {
                        //end = mid - 1;
                        begin = mid + 1;
                    }
                    else if(target > arr[begin]) {
                        //begin = mid + 1;
                        end = mid - 1;
                    }
                    else {
                        result = begin;
                    }

                }
                else{
                    if(target < arr[end]) {
                        //end = mid - 1;
                        begin = mid + 1;
                    }
                    else if(target > arr[end]) {
                        //begin = mid + 1;
                        end = mid - 1;
                    }
                    else {
                        result = end;
                    }
                }
            }
            return result;
        }


    public static void main(String[] args){
        Integer[] arr = new Integer[8];
        arr[0] = 4;
        arr[1] = 5;
        arr[2] = 6;
        arr[3] = 7;
        arr[4] = 0;
        arr[5] = 1;
        arr[6] = 2;
        arr[7] = 3;
        //4567 0123
        System.out.println(binSearch3(arr,0,7,1));
        System.out.println(binSearch3(arr,0,7,2));
        System.out.println(binSearch3(arr,0,7,3));
        System.out.println(binSearch3(arr,0,7,4));
        System.out.println(binSearch3(arr,0,7,5));
    }

}
