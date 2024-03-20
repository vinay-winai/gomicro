package mm

import (
	"context"
	"database/sql"
	"errors"
	"github.com/google/uuid"
	"github.com/vinay-winai/gomicro/internal/producer"
	pb "github.com/vinay-winai/gomicro/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	insertTransactionQuery = "INSERT INTO transaction (pid, src_user_id, dst_user_id, src_wallet_id, dst_wallet_id, src_account_id, dst_account_id, src_account_type, dst_account_type, final_dst_merchant_wallet_id, amount) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
	selectTransactionQuery = "SELECT id, pid, src_user_id, dst_user_id, src_wallet_id, dst_wallet_id, src_account_id, dst_account_id, src_account_type, dst_account_type, final_dst_merchant_wallet_id, amount FROM transaction WHERE pid=?"
)
type Implementation struct {
	db *sql.DB
	pb.UnimplementedMoneyMovementServiceServer
}

func NewMoneyMovementImplementation(db *sql.DB) *Implementation {
	return &Implementation{db: db}
}

func (impl *Implementation) Authorize(ctx context.Context, authorizePayload *pb.AuthorizePayload) (*pb.AuthorizeResponse, error) {
	if authorizePayload.GetCurrency() != "USD" {
		return nil, status.Error(codes.InvalidArgument, "only accepts USD")
	}
	// Begin transaction
	tx, err := impl.db.Begin()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	merchantWallet, err := fetchWallet(tx, authorizePayload.MerchantWalletUserId)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	customerWallet, err := fetchWallet(tx, authorizePayload.CustomerWalletUserId)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	srcAccount, err := fetchAccount(tx, customerWallet.ID, "DEFAULT")
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	dstAccount, err := fetchAccount(tx, customerWallet.ID, "PAYMENT")
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}
	
	err = transfer(tx, srcAccount, dstAccount, authorizePayload.Cents)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	pid := uuid.NewString()
	err = createTransaction(tx, pid, srcAccount, dstAccount, customerWallet, customerWallet, merchantWallet, authorizePayload.Cents)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	//End transaction
	err = tx.Commit()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.AuthorizeResponse{Pid: pid}, nil
}

func (impl *Implementation) Capture(ctx context.Context, capturePayload *pb.CapturePayload) (*emptypb.Empty, error) {

	// Begin transaction
	tx, err := impl.db.Begin()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	authorizeTransaction, err := fetchTransaction(tx, capturePayload.Pid)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	srcAccount, err := fetchAccount(tx, authorizeTransaction.dstAccountWalletID, "PAYMENT")
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}
	
	dstMerchantAccount, err := fetchAccount(tx, authorizeTransaction.finalDstMerchantWalletID, "INCOMING")
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}
	
	err = transfer(tx, srcAccount, dstMerchantAccount, authorizeTransaction.amount)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}
	
	customerWallet, err := fetchWallet(tx, authorizeTransaction.srcUserID)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}
	
	merchantWallet, err := fetchWalletWithWalletID(tx, authorizeTransaction.finalDstMerchantWalletID)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}
	
	err = createTransaction(tx, authorizeTransaction.pid,  srcAccount, dstMerchantAccount, customerWallet, merchantWallet, merchantWallet, authorizeTransaction.amount)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}
	// commit transaction
	err = tx.Commit()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	producer.SendCaptureMessage(authorizeTransaction.pid, authorizeTransaction.srcUserID, authorizeTransaction.amount)
	
	return &emptypb.Empty{}, nil
}

func fetchWallet(tx *sql.Tx, userID string) (wallet, error) {
	var w wallet
	stmt, err := tx.Prepare("SELECT id, user_id, wallet_type FROM wallet WHERE user_id = ?")
	if err != nil {
		return w, status.Error(codes.Internal, err.Error())
	}
	err = stmt.QueryRow(userID).Scan(&w.ID, &w.userID, &w.walletType)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return w, status.Error(codes.InvalidArgument, err.Error())
		}
		return w, status.Error(codes.Internal, err.Error())
	}
	return w, nil
}

func fetchAccount(tx *sql.Tx, walletID int32, accountType string) (account, error) {
	var a account
	stmt, err := tx.Prepare("SELECT id, cents, account_type, wallet_id FROM account WHERE wallet_id = ? AND account_type = ?")
	if err != nil {
		return a, status.Error(codes.Internal, err.Error())
	}
	err = stmt.QueryRow(walletID, accountType).Scan(&a.ID, &a.cents, &a.accountType, &a.walletID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return a, status.Error(codes.InvalidArgument, err.Error())
		}
		return a, status.Error(codes.Internal, err.Error())
	}
	return a, nil
}

func transfer(tx *sql.Tx, srcAccount account, dstAccount account, amount int64) error {
	if srcAccount.cents < amount {
		return status.Error(codes.InvalidArgument, "insufficient funds")
	}
	// Subtract money from src
	stmt, err := tx.Prepare("UPDATE account SET cents = ? WHERE id = ?")
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	_, err = stmt.Exec(srcAccount.cents-amount, srcAccount.ID)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	// Add money to dst
	_, err = stmt.Exec(dstAccount.cents+amount, dstAccount.ID)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	return nil
}

func createTransaction(tx *sql.Tx, pid string, srcAccount account, dstAccount account, srcWallet wallet, dstWallet wallet, finalDstWallet wallet, amount int64) (error) {
	stmt, err := tx.Prepare(insertTransactionQuery)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	_, err = stmt.Exec(pid, srcWallet.userID, dstWallet.userID, srcWallet.ID, dstWallet.ID, srcAccount.ID, dstAccount.ID, srcAccount.accountType, dstAccount.accountType, finalDstWallet.ID, amount)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	return nil
}

func fetchTransaction(tx *sql.Tx, pid string) (transaction, error) {
	var t transaction
	stmt, err := tx.Prepare(selectTransactionQuery)
	if err != nil {
		return t, status.Error(codes.Internal, err.Error())
	}
	err = stmt.QueryRow(pid).Scan(&t.ID, &t.pid, &t.srcUserID, &t.dstUserID, &t.srcAccountWalletID, &t.dstAccountWalletID, &t.srcAccountID, &t.dstAccountID, &t.srcAccountType, &t.dstAccountType, &t.finalDstMerchantWalletID, &t.amount)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return t, status.Error(codes.InvalidArgument, err.Error())
		}
		return t, status.Error(codes.Internal, err.Error())
	}
	return t, nil
}

func fetchWalletWithWalletID(tx *sql.Tx, walletID int32) (wallet, error) {
	var w wallet
	stmt, err := tx.Prepare("SELECT id, user_id, wallet_type FROM wallet WHERE id = ?")
	if err != nil {
		return w, status.Error(codes.Internal, err.Error())
	}
	err = stmt.QueryRow(walletID).Scan(&w.ID, &w.userID, &w.walletType)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return w, status.Error(codes.InvalidArgument, err.Error())
		}
		return w, status.Error(codes.Internal, err.Error())
	}
	return w, nil
}