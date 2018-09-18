// +build amd64,!appengine,!gccgo
// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package roaring implements roaring bitmaps with support for incremental changes.
// Frame layout
//	|-----------------------------+---+---+---+---|
// 0	| a_data_ptr                  |   |   |   |   |
// 8	| a_len                       |   |   |   |   |
// 16	| a_cap                       |   |   |   |   |
// 24	| b_data_ptr                  |   |   |   |   |
// 32	| b_len                       |   |   |   |   |
// 40	| b_cap                       |   |   |   |   |
// 48	| c_data_ptr                  |   |   |   |   |
// 56	| c_len                       |   |   |   |   |
// 64	| c_cap                       |   |   |   |   |
// 72	| function return value (int) |   |   |   |   |
//
// func asmAnd(a,b,c []int64)int
TEXT ·asmAnd(SB), 7, $0
	MOVQ $0, SI
	MOVQ a_data+0(FP), BX   // BX = &a[0]
	MOVL a_len+8(FP), CX    // len(a)
	MOVQ b_data+24(FP), DX  // DX = &b[0]
	MOVQ c_data+48(FP), R15 // DX = &c[0]
        VPXORQ Z2,Z2,Z2

	VMOVDQA64 (BX), Z0
	VMOVDQA64 (DX), Z1
	VPANDQ   Z0, Z1, Z0
	VMOVUPS Z0, (R15)
        VPOPCNTQ Z0,Z0
        VPADDQ Z0,Z2,Z2	

	VMOVDQA64 64(BX), Z0
	VMOVDQA64 64(DX), Z1
	VPANDQ   Z0, Z1, Z0
	VMOVUPS Z0, 64(R15)
        VPOPCNTQ Z0,Z0
        VPADDQ Z0,Z2,Z2	

	VMOVDQA64 128(BX), Z0
	VMOVDQA64 128(DX), Z1
	VPANDQ   Z0, Z1, Z0
	VMOVUPS Z0, 128(R15)
        VPOPCNTQ Z0,Z0
        VPADDQ Z0,Z2,Z2	

	VMOVDQA64 192(BX), Z0
	VMOVDQA64 192(DX), Z1
	VPANDQ   Z0, Z1, Z0
	VMOVUPS Z0, 192(R15)
        VPOPCNTQ Z0,Z0
        VPADDQ Z0,Z2,Z2	

	VMOVDQA64 256(BX), Z0
	VMOVDQA64 256(DX), Z1
	VPANDQ   Z0, Z1, Z0
	VMOVUPS Z0, 256(R15)
        VPOPCNTQ Z0,Z0
        VPADDQ Z0,Z2,Z2	

	VMOVDQA64 320(BX), Z0
	VMOVDQA64 320(DX), Z1
	VPANDQ   Z0, Z1, Z0
	VMOVUPS Z0, 320(R15)
        VPOPCNTQ Z0,Z0
        VPADDQ Z0,Z2,Z2	

	VMOVDQA64 384(BX), Z0
	VMOVDQA64 384(DX), Z1
	VPANDQ   Z0, Z1, Z0
	VMOVUPS Z0, 384(R15)
        VPOPCNTQ Z0,Z0
        VPADDQ Z0,Z2,Z2	

	VMOVDQA64 448(BX), Z0
	VMOVDQA64 448(DX), Z1
	VPANDQ   Z0, Z1, Z0
	VMOVUPS Z0, 448(R15)
        VPOPCNTQ Z0,Z0
        VPADDQ Z0,Z2,Z2	

	VMOVDQA64 512(BX), Z0
	VMOVDQA64 512(DX), Z1
	VPANDQ   Z0, Z1, Z0
	VMOVUPS Z0, 512(R15)
        VPOPCNTQ Z0,Z0
        VPADDQ Z0,Z2,Z2	

	VMOVDQA64 576(BX), Z0
	VMOVDQA64 576(DX), Z1
	VPANDQ   Z0, Z1, Z0
	VMOVUPS Z0, 576(R15)
        VPOPCNTQ Z0,Z0
        VPADDQ Z0,Z2,Z2	

	VMOVDQA64 640(BX), Z0
	VMOVDQA64 640(DX), Z1
	VPANDQ   Z0, Z1, Z0
	VMOVUPS Z0, 640(R15)
        VPOPCNTQ Z0,Z0
        VPADDQ Z0,Z2,Z2	

	VMOVDQA64 704(BX), Z0
	VMOVDQA64 704(DX), Z1
	VPANDQ   Z0, Z1, Z0
	VMOVUPS Z0, 704(R15)
        VPOPCNTQ Z0,Z0
        VPADDQ Z0,Z2,Z2	

	VMOVDQA64 768(BX), Z0
	VMOVDQA64 768(DX), Z1
	VPANDQ   Z0, Z1, Z0
	VMOVUPS Z0, 768(R15)
        VPOPCNTQ Z0,Z0
        VPADDQ Z0,Z2,Z2	

	VMOVDQA64 832(BX), Z0
	VMOVDQA64 832(DX), Z1
	VPANDQ   Z0, Z1, Z0
	VMOVUPS Z0, 832(R15)
        VPOPCNTQ Z0,Z0
        VPADDQ Z0,Z2,Z2	

	VMOVDQA64 896(BX), Z0
	VMOVDQA64 896(DX), Z1
	VPANDQ   Z0, Z1, Z0
	VMOVUPS Z0, 896(R15)
        VPOPCNTQ Z0,Z0
        VPADDQ Z0,Z2,Z2	

	VMOVDQA64 960(BX), Z0
	VMOVDQA64 960(DX), Z1
	VPANDQ   Z0, Z1, Z0
	VMOVUPS Z0, 960(R15)
        VPOPCNTQ Z0,Z0
        VPADDQ Z0,Z2,Z2	
//now i gotta figure out how to get the sum out of Z2

	MOVQ    SI, ·noname+72(FP)
	VZEROUPPER
	RET

// func asmOr(a,b,c []int64)int
TEXT ·asmOr(SB), 7, $0
	MOVQ $0, SI
	MOVQ a_data+0(FP), BX   // BX = &a[0]
	MOVL a_len+8(FP), CX    // len(a)
	MOVQ b_data+24(FP), DX  // DX = &b[0]
	MOVQ c_data+48(FP), R15 // DX = &c[0]

loop_begin1:
	VMOVDQA (BX), Y0
	VMOVDQA (DX), Y1
	VPOR    Y0, Y1, Y0
	VMOVUPS Y0, (R15)

	POPCNTQ (R15), BP
	ADDQ    BP, SI
	POPCNTQ 8(R15), BP
	ADDQ    BP, SI
	POPCNTQ 16(R15), BP
	ADDQ    BP, SI
	POPCNTQ 24(R15), BP
	ADDQ    BP, SI

	ADDQ $32, BX
	ADDQ $32, DX
	ADDQ $32, R15
	SUBQ $4, CX
	JNE  loop_begin1
	MOVQ SI, ·noname+72(FP)
	VZEROUPPER
	RET

// func asmXor(a,b,c []int64)int
TEXT ·asmXor(SB), 7, $0
	MOVQ $0, SI
	MOVQ a_data+0(FP), BX   // BX = &a[0]
	MOVL a_len+8(FP), CX    // len(a)
	MOVQ b_data+24(FP), DX  // DX = &b[0]
	MOVQ c_data+48(FP), R15 // DX = &c[0]

loop_begin2:
	VMOVDQA (BX), Y0
	VMOVDQA (DX), Y1
	VPXOR   Y0, Y1, Y0
	VMOVUPS Y0, (R15)

	POPCNTQ (R15), BP
	ADDQ    BP, SI
	POPCNTQ 8(R15), BP
	ADDQ    BP, SI
	POPCNTQ 16(R15), BP
	ADDQ    BP, SI
	POPCNTQ 24(R15), BP
	ADDQ    BP, SI

	ADDQ $32, BX
	ADDQ $32, DX
	ADDQ $32, R15
	SUBQ $4, CX
	JNE  loop_begin2
	MOVQ SI, ·noname+72(FP)
	VZEROUPPER
	RET

// func asmAndN(a,b,c []int64)int
TEXT ·asmAndN(SB), 7, $0
	MOVQ $0, SI
	MOVQ a_data+0(FP), BX   // BX = &a[0]
	MOVL a_len+8(FP), CX    // len(a)
	MOVQ b_data+24(FP), DX  // DX = &b[0]
	MOVQ c_data+48(FP), R15 // DX = &c[0]

loop_begin3:
	VMOVDQA64 (BX), Z0
	VMOVDQA64 (DX), Z1
	VPANDNQ  Z0, Z1, Z0
	VMOVUPS Z0, (R15)

//	POPCNTQ (R15), BP
//	ADDQ    BP, SI
//	POPCNTQ 8(R15), BP
//	ADDQ    BP, SI
//	POPCNTQ 16(R15), BP
//	ADDQ    BP, SI
//	POPCNTQ 24(R15), BP
//	ADDQ    BP, SI

	ADDQ $64, BX
	ADDQ $64, DX
	ADDQ $64, R15
	SUBQ $8, CX
	JNE  loop_begin3
	MOVQ SI, ·noname+72(FP)
	VZEROUPPER
	RET
