#include "rfs.h"
#include "fs.h"
#include <chrono>

std::vector<uint8_t> rfs::downloadBuffer;

void rfs::writeThread_t(void *a)
{
    rfs::dlWriteThreadStruct *in = (rfs::dlWriteThreadStruct *)a;
    std::vector<uint8_t> localBuff;
    unsigned written = 0;

    FILE *out = fopen(in->cfa->path.c_str(), "wb");
    if (!out) {
        in->error = true;
        fs::logWrite("WebDav: 无法打开输出文件: %s\n", in->cfa->path.c_str());
        return;
    }

    while(!in->abort && (written < in->cfa->size || in->cfa->size == 0))
    {
        std::unique_lock<std::mutex> dataLock(in->dataLock);
        // 添加超时机制，避免无限等待 - 5秒超时
        if (!in->cond.wait_for(dataLock, std::chrono::seconds(5), [in]{
            return in->bufferFull || in->abort || in->completed;
        })) {
            // 超时检查 - 如果下载进度长时间没变化，可能是卡住了
            if (!in->sharedBuffer.empty() || in->error) {
                // 有数据或发生错误，继续处理
            } else {
                continue; // 继续等待
            }
        }
        
        // 检查是否需要终止
        if (in->abort) {
            break;
        }
        
        // 如果缓冲区为空但已完成，跳出循环
        if (!in->bufferFull && in->completed) {
            break;
        }

        localBuff.clear();
        localBuff.assign(in->sharedBuffer.begin(), in->sharedBuffer.end());
        in->sharedBuffer.clear();
        in->bufferFull = false;
        dataLock.unlock();
        in->cond.notify_one();

        if (!localBuff.empty()) {
            written += fwrite(localBuff.data(), 1, localBuff.size(), out);
        }
    }
    
    fclose(out);
    rfs::downloadBuffer.clear();
    in->completed = true;
}

size_t rfs::writeDataBufferThreaded(uint8_t *buff, size_t sz, size_t cnt, void *u)
{
    rfs::dlWriteThreadStruct *in = (rfs::dlWriteThreadStruct *)u;
    
    // 检查是否已中止
    if (in->abort) {
        return 0;
    }
    
    // 添加数据到下载缓冲区
    rfs::downloadBuffer.insert(rfs::downloadBuffer.end(), buff, buff + (sz * cnt));
    in->downloaded += sz * cnt;

    // 当达到目标大小或缓冲区满时，传输数据给写入线程
    if(in->cfa->size > 0 && in->downloaded >= in->cfa->size || rfs::downloadBuffer.size() >= DOWNLOAD_BUFFER_SIZE)
    {
        std::unique_lock<std::mutex> dataLock(in->dataLock);
        // 添加超时机制，避免无限等待 - 5秒超时
        if (!in->cond.wait_for(dataLock, std::chrono::seconds(5), [in]{
            return in->bufferFull == false || in->abort;
        })) {
            // 超时但仍未解除阻塞
            if (!in->abort) {
                in->error = true;
                fs::logWrite("WebDav: 下载线程响应超时\n");
            }
            return 0; // 中断传输
        }
        
        // 再次检查是否已中止
        if (in->abort) {
            return 0;
        }
        
        in->sharedBuffer.assign(rfs::downloadBuffer.begin(), rfs::downloadBuffer.end());
        rfs::downloadBuffer.clear();
        in->bufferFull = true;
        dataLock.unlock();
        in->cond.notify_one();
    }

    // 更新进度
    if(in->cfa->o)
        *in->cfa->o = in->downloaded;

    return sz * cnt;
}
