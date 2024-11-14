//
// Created by zxk on 5/20/23.
//

#ifndef OLVP_ARROWEXPRNODE_HPP
#define OLVP_ARROWEXPRNODE_HPP
class ArrowExprNode
{
    std::shared_ptr<gandiva::Node> exprNode = NULL;
public:
    ArrowExprNode( std::shared_ptr<gandiva::Node> exprNode)
    {
        this->exprNode = exprNode;
    }

    std::shared_ptr<gandiva::Node> get()
    {
        return this->exprNode;
    }

    bool isNull()
    {
        if(exprNode == NULL)
            return true;
        else
            return false;
    }

};

#endif //OLVP_ARROWEXPRNODE_HPP
