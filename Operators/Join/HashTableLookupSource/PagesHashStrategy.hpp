//
// Created by zxk on 5/22/23.
//

#ifndef OLVP_PAGESHASHSTRATEGY_HPP
#define OLVP_PAGESHASHSTRATEGY_HPP
#include "../../../Page/DataPage.hpp"
#include "../../../Page/DataPageBuilder.hpp"
class PagesHashStrategy
{
public:
    PagesHashStrategy(){

    }
    virtual int32_t getChannelCount(){return 0;}

    virtual int64_t getSizeInBytes(){return 0;}


    virtual void appendTo(int position, std::shared_ptr<DataPageBuilder> pageBuilder, int outputChannelOffset) = 0;


    virtual int64_t hashPosition(int position){return 0;}


    virtual int64_t hashRow(int position, std::shared_ptr<DataPage> page){return 0;}


    virtual bool rowEqualsRow(int leftPosition, std::shared_ptr<DataPage> leftPage, int rightPosition, std::shared_ptr<DataPage> rightPage){return false;}


    virtual bool positionEqualsRow(int leftPosition, int rightPosition, std::shared_ptr<DataPage> rightPage){return 0;}


    virtual bool positionEqualsRowIgnoreNulls(int leftPosition, int rightPosition, std::shared_ptr<DataPage> rightPage){return 0;}

    virtual bool positionEqualsRow(int leftPosition, int rightPosition, std::shared_ptr<DataPage> page, int *rightChannels){return 0;}


    virtual bool positionNotDistinctFromRow(int leftPosition, int rightPosition, std::shared_ptr<DataPage> page, int *rightChannels){return false;}

    virtual bool positionEqualsPosition(int leftPosition, int rightPosition){return false;}


    virtual bool positionEqualsPositionIgnoreNulls(int leftPosition,int rightPosition){return false;}


    virtual bool isPositionNull(int blockPosition){return false;}


    virtual int32_t compareSortChannelPositions(int leftBlockPosition, int rightBlockPosition){return 0;}


    virtual bool isSortChannelPositionNull(int blockPosition){return false;}
};
#endif //OLVP_PAGESHASHSTRATEGY_HPP
