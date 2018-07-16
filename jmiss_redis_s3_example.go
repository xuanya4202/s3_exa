package main

import (
//	"flag"
	"fmt"
//	"github.com/aws/aws-sdk-go/aws"
//	"github.com/aws/aws-sdk-go/service/s3"
//	"os"
//	"s3client"
	//	"strings"
//	"sync"
	//	"sync/atomic"
	//	"container/list"
//	"crypto/md5"
//	"time"
	"s3storage"
)

func main(){
	
    s3Storage := s3storage.NewS3Storage("4034CAC03D9B83501CDACA21E939B07F", "A6B9EDB77B6EC26B2E8E836F3F9A3936", "s3.cn-north-1.jcloudcs.com") 
	
	// small file
	result,err := s3Storage.PutObjectFromFile("jmiss-redis-backup", "small", "/root/test/small")
	if nil != err{
		fmt.Printf(" put small object from file failed! err:%v \n", err)
		return
	}
	fmt.Printf(" put small object from file ok! size:%d md5:%s \n", result.Size, result.Md5)

	err = s3Storage.GetObjectToFile("jmiss-redis-backup", "small", "/root/test/smallget", result)
	if nil != err{
		fmt.Printf(" get small object to file failed! err:%v \n", err)
		return
	}
	fmt.Printf(" get small object to file ok!\n")
	
	err = s3Storage.DeleteObject("jmiss-redis-backup", "small")
	if nil != err{
		fmt.Printf(" delete small object err:%v \n", err)
		return
	}
	fmt.Printf(" delete small object ok!\n")
	
	
	// big file
	result,err = s3Storage.PutObjectFromFile("jmiss-redis-backup", "big", "/root/test/big")
	if nil != err{
		fmt.Printf(" put big object from file failed! err:%v \n", err)
		return
	}
	fmt.Printf(" put big object from file ok! size:%d md5:%s \n", result.Size, result.Md5)

	err = s3Storage.GetObjectToFile("jmiss-redis-backup", "big", "/root/test/bigget", result)
	if nil != err{
		fmt.Printf(" get big object to file failed! err:%v \n", err)
		return
	}
	fmt.Printf(" get big object to file ok!\n")
	
	err = s3Storage.DeleteObject("jmiss-redis-backup", "big")
	if nil != err{
		fmt.Printf(" delete big object err:%v \n", err)
		return
	}
	fmt.Printf(" delete big object ok!\n")
}
