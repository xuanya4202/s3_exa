package s3storage

import (
	//"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"os"
	"io"
	"s3client"
	//	"strings"
	//"sync"
	//	"sync/atomic"
	//	"container/list"
	"crypto/md5"
	"encoding/hex"
	//"time"
	"errors"
	"bytes"
)

type S3Storage struct{
	accessKeyId string
	accessKeySecret string
	s3Url string
}
type PutObjectResult struct {
	Size uint64
	Md5  string
}
func NewS3Storage(accessKeyId string, accessKeySecret string, s3Url string) *S3Storage{
	s3Storage := &S3Storage{}
	s3Storage.SetAccessKeyId(accessKeyId)
    s3Storage.SetAccessKeySecret(accessKeySecret)
    s3Storage.SetS3Url(s3Url)
    return s3Storage
}

func (s3Storage *S3Storage)SetAccessKeyId(accessKeyId string){
	s3Storage.accessKeyId = accessKeyId
	return
}
func (s3Storage *S3Storage)SetAccessKeySecret(accessKeySecret string){
	s3Storage.accessKeySecret = accessKeySecret
	return
}
func (s3Storage *S3Storage)SetS3Url(s3Url string){
	s3Storage.s3Url = s3Url
	return
}

func PutObject(sess *s3.S3, ObjFile *os.File, bucket string, object string, size uint64) (*PutObjectResult, error) {
	var err error
	n := 0
	buf := make([]byte, size)
	n, err = ObjFile.Read(buf)

	if nil != err {
		return nil, err
	}
	if uint64(n) != size {
		return nil, errors.New(fmt.Sprintf("read failed! size:%d n:%d", size, n))
	}
	params := &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &object,
		Body:   bytes.NewReader([]byte(buf)),
	}

	_, err = sess.PutObject(params)
	if err != nil {
		return nil, err
	}

	md5Ctx := md5.New()
	md5Ctx.Write(buf)
	cipherStr := md5Ctx.Sum(nil)
	md5String := hex.EncodeToString(cipherStr)

	result := &PutObjectResult{
		Size: size,
		Md5:  md5String,
	}
	return result, nil
}

func uploadPart(sess *s3.S3, )

func uploadObject(sess *s3.S3, ObjFile *os.File, bucket string, object string, size uint64) (*PutObjectResult, error) {

	var chunkSize uint64
	var etags []*string
	var haveRead uint64
	var needRead uint64
	
	chunkSize = 8*1024*1024
	haveRead = 0 
	needRead = 0

	params := &s3.CreateMultipartUploadInput{
		Bucket:		&bucket,
		Key:		&object,
	}
	
	resp, err := sess.CreateMultipartUpload(params)
	if nil != err{
		return nil, err
	}
	mpuId := resp.UploadId
	
	if size - haveRead > chunkSize {
		needRead = chunkSize
	}else{
		needRead = size - haveRead
	}

	partNum  := 1
	md5Ctx := md5.New()
	for{
		if needRead == 0{
			break
		}
		
		buf := make([]byte, needRead)
		n := 0
		n, err = ObjFile.Read(buf)

		if nil != err {
			break
		}
		if uint64(n) != needRead {
			break
		}

		upParams := s3.UploadPartInput{
			Bucket:		&bucket,
			Key:		&object,
			PartNumber:	aws.Int64(int64(partNum)),
			UploadId:	mpuId,
			Body:   bytes.NewReader([]byte(buf)),
		}
		
		var upResp *s3.UploadPartOutput
		upResp, err = sess.UploadPart(&upParams)
		if nil != err{
			break
		}
		etags = append(etags, upResp.ETag)
		md5Ctx.Write(buf)
		haveRead = haveRead + needRead
		if size - haveRead > chunkSize {
			needRead = chunkSize
		}else{
			needRead = size - haveRead
		}
		partNum  = partNum + 1
	}

	if nil != err {
		return nil, err
	}

	var parts [] *s3.CompletedPart
	for i, v := range etags{
		parts = append(parts, &s3.CompletedPart{
			ETag:		v,
			PartNumber:	aws.Int64(int64(i + 1)),
		})
	}

	cmuParams := &s3.CompleteMultipartUploadInput{
		Bucket:			&bucket,
		Key:			&object,
		UploadId:		mpuId,
		MultipartUpload:	&s3.CompletedMultipartUpload{
			Parts:		parts,
		},
	}

	_, err = sess.CompleteMultipartUpload(cmuParams)
	if nil != err {
		return nil, err
	}

	cipherStr := md5Ctx.Sum(nil)
	md5String := hex.EncodeToString(cipherStr)
	result := &PutObjectResult{
		Size: size,
		Md5:  md5String,
	}
	return result, nil
}
func (s3Storage *S3Storage) PutObjectFromFile(bucket string, object string, filePath string) (*PutObjectResult, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	size := stat.Size()
	fmt.Printf("accessKeyId:%s", s3Storage.accessKeyId, " accessKeySecret:%s", s3Storage.accessKeySecret, " s3Url:%s\n", s3Storage.s3Url)
	sess := s3client.NewS3Client(s3Storage.accessKeyId, s3Storage.accessKeySecret, s3Storage.s3Url)
	if 8*1024*1024 > size {
		//put
		return PutObject(sess, file, bucket, object, uint64(size))
	} else {
		//multi
		return uploadObject(sess, file, bucket, object, uint64(size))
	}
}

func (s3Storage *S3Storage)DeleteObject(bucket string, object string) (error) {
	sess := s3client.NewS3Client(s3Storage.accessKeyId, s3Storage.accessKeySecret, s3Storage.s3Url)
	params := &s3.DeleteObjectInput{
		Bucket: 	&bucket,
		Key:		&object,
	}

	_, err := sess.DeleteObject(params)
	if err != nil{
		return err
	}
	return nil
}

func (s3Storage *S3Storage)GetObjectToFile(bucket string, object string, filePath string, putObjectResult *PutObjectResult) (error) {
	var err error
	var objFile *os.File
	objFile, err = os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer objFile.Close()

	sess := s3client.NewS3Client(s3Storage.accessKeyId, s3Storage.accessKeySecret, s3Storage.s3Url)
	params := &s3.GetObjectInput{
		Bucket:  &bucket,
		Key:	 &object,
	}
	req,resp := sess.GetObjectRequest(params)
	err = req.Send()
	if err != nil{
		return err
	}
	defer resp.Body.Close()

	contenLength := *resp.ContentLength
	if uint64(contenLength) != putObjectResult.Size {
		return errors.New(fmt.Sprintf("mismatched length! s3 length:%d size:%d ", contenLength, putObjectResult.Size))
	}

	if 0 == contenLength{
		return nil
	}
	
	md5Ctx := md5.New()
	var written int64
	buf := make([]byte, 32*1024)
	for{
		nr,er := resp.Body.Read(buf)
		if nr > 0{
			nw,ew := objFile.Write(buf[0:nr])
			if nw > 0{
				written += int64(nw)
			}
			if ew != nil{
				err = ew
				break
			}
			if nr != nw{
				err = io.ErrShortWrite
				break
			}
			md5Ctx.Write(buf[0:nr])
		}
		if er == io.EOF{
			break
		}
		if er != nil{
			err = er
			break
		}
	}
	if nil != err{
		return err
	}
	if contenLength != written{
		return errors.New(fmt.Sprintf("read s3 mismatched length! s3 length:%d written:%d ", contenLength, contenLength))
	}

	cipherStr := md5Ctx.Sum(nil)
	md5String := hex.EncodeToString(cipherStr)

	if md5String != putObjectResult.Md5{
		return errors.New(fmt.Sprintf("read s3 mismatched MD5! s3 md5:%s src md5:%s ", md5String, putObjectResult.Md5))
	}
	return nil
}
