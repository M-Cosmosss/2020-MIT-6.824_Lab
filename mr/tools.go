package mr

import "log"

const chunkNums=10

func Chk(err error){
	if err!=nil{
		log.Fatal(err)
	}
}
//func split(file []string){
//	i,nums:=0,0
//	sum:=make([]byte,0)
//	for _,j:=range file{
//		tmp,e:=ioutil.ReadFile(j)
//		if e!=nil{
//			log.Fatal(e)
//		}
//		sum=append(sum,tmp...)
//	}
//	for _,j:= range sum {
//		if j==' '||j=='\n' {
//			i++
//		}
//	}
//}
