package roaring
/*
#cgo CFLAGS: -march=native

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <memory.h>
#include <stdint.h>
#include <x86intrin.h>

int avx_and( long long unsigned int* a_ptr, long long unsigned int* b_ptr, long long unsigned int* result){

   const long long unsigned int* a_ptr_end = a_ptr + 1024;
   long long unsigned int temp[8];
   int i=0;
   while (a_ptr < a_ptr_end) {
      __m512i aa = _mm512_setr_epi64(a_ptr[7],a_ptr[6],a_ptr[5],a_ptr[4],a_ptr[3], a_ptr[2], a_ptr[1], a_ptr[0]);
      __m512i bb = _mm512_setr_epi64(b_ptr[7],b_ptr[6],b_ptr[5],b_ptr[4],b_ptr[3], b_ptr[2], b_ptr[1], b_ptr[0]);
       _mm512_store_si512((__m512i *)&temp[0], _mm512_and_si512(aa, bb));
      result[i++]=temp[7];
      result[i++]=temp[6];
      result[i++]=temp[5];
      result[i++]=temp[4];
      result[i++]=temp[3];
      result[i++]=temp[2];
      result[i++]=temp[1];
      result[i++]=temp[0];
      a_ptr += 8;
      b_ptr += 8;
   }
   return 0;
}

long long unsigned int* avx_allocBitmap(){
       long long unsigned int* vals;
       if (posix_memalign((void**)(&vals), 64,8192) !=0){
       };
      return vals;
}

*/
import "C"
import (
	"unsafe"
	"reflect"
)	

func avxNewBitmap() []uint64 {
	ptr := C.avx_allocBitmap()
	hdr := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(ptr)),
		Len:  1024,
		Cap:  1024,
	}
	goSlice := *(*[]uint64)(unsafe.Pointer(&hdr))
	return goSlice
}
func avxFreeBitmap(v[]uint64){
	C.free(unsafe.Pointer(&v[0]))
}

func And(a,b[]uint64)[]uint64{
	a1 :=avxNewBitmap()
	b1 :=avxNewBitmap()
	c1 :=avxNewBitmap()
	copy(a1,a)
	copy(b1,b)
	C.avx_and( (*C.ulonglong)(&a1[0]), 
	           (*C.ulonglong)(&b1[0]), 
		   (*C.ulonglong)(&c1[0]),
	          )
	results:=make([]uint64,1024,1024)
	copy(results,c1)
	avxFreeBitmap(a1)
	avxFreeBitmap(b1)
	avxFreeBitmap(c1)
	return results
}
